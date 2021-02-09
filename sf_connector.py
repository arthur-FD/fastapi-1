import jwt
import azure.functions as func
from fastapi import FastAPI, Depends, status, Query,Body
from pydantic import BaseModel, create_model
from typing import Optional,List,Dict,Tuple,Sequence
import yaml
import jwt
from utils.config_loader import ConfigLoader
import snowflake.connector
import os
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.hash import bcrypt
from tortoise import fields 
from tortoise.contrib.fastapi import register_tortoise
from tortoise.contrib.pydantic import pydantic_model_creator
from tortoise.models import Model 
from utils.processing_functions import *
from main import app,get_api_key,APIKey

class filter_sql(BaseModel):
    column: str
    value_filter: Tuple[str,...]= tuple()

class API_params(BaseModel):
    columns: List[str]
    filters: List[filter_sql]
    graph_columns: Optional[List[str]]
    metrics: List[str]
    granularity: Tuple[str,...]
    
def sort_model(filters: List[filter_sql] = Body(...), columns: List[str] = Body(...),graph_columns: Optional[List[str]] = Body(...),metrics: List[str] = Body(...),granularity: Tuple[str,...] = Body(...),api_key: APIKey = Depends(get_api_key)):
    return API_params(filters=filters, columns=columns,graph_columns=graph_columns,metrics=metrics,granularity=granularity)

@app.post("/custom_view", tags=["test"])
def get_sort(params: API_params = Depends(sort_model)):
    print(params.metrics)
    print(params.granularity)
    print(params.filters)
    with open("conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open("conf/mapping_columns.yml", "r") as file:
        mapping = yaml.load(file, Loader=ConfigLoader)
    columns_string=','.join(map(lambda x: f'{mapping[x]}.{x}' ,params.columns))
    filters_list=params.filters
    where='WHERE '+' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=[]])
    query=f'''
        SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
        FROM EV_VOLUMES_TEST
        INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE {where}
        GROUP BY  {columns_string}, EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY 
    '''
    print(query)
    conn = snowflake.connector.connect(
        user=os.environ["USER_SF"],
        password=os.environ["PSW_SF"],
        account=os.environ["ACCOUNT_SF"],
        **parameters["snowflake_config"]
    ) 
    
    cur = conn.cursor()
    cur.execute(query)
    core_data = cur.fetch_pandas_all()
    dict_TD={}
    if 'YEAR' in params.granularity or 'QUARTER' in params.granularity:
        last_year=core_data[core_data.PERIOD_GRANULARITY=='YEAR'].DATE.max().year
        last_month=core_data[core_data.PERIOD_GRANULARITY=='YEAR'].DATE.max().month
        query_month_availables=f'''select MONTHS_AVAILABLE FROM CONFIG where YEAR={last_year}'''
        print(query_month_availables)
        cur = conn.cursor()
        cur.execute(query_month_availables)
        nb_months = cur.fetch_pandas_all()['MONTHS_AVAILABLE'].iloc[0]
        print(nb_months)
        
        if 0<nb_months<=12:     
            if 'YEAR' in params.granularity:
                where='WHERE '+' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=[] and filter_sql.column!= 'PERIOD_GRANULARITY'])
                where+=f''' and EV_VOLUMES_TEST.PERIOD_GRANULARITY = 'MONTH' and EXTRACT(YEAR FROM EV_VOLUMES_TEST.DATE)={last_year} and EXTRACT(MONTH FROM EV_VOLUMES_TEST.DATE) in {tuple(range(0,int(nb_months+1)))} ''' 
                query_ytd=f'''
                SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
                FROM EV_VOLUMES_TEST
                INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE {where}
                GROUP BY  {columns_string}, EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY 
                '''           
                cur = conn.cursor()
                cur.execute(query_ytd)
                data_ytd = process_data(cur.fetch_pandas_all(),params.columns,['MONTH']).set_index(params.columns).sum(axis=1)
                print(data_ytd)
                dict_TD[f'{last_year}YTD']=pd.DataFrame(data_ytd,columns=[f'{last_year}YTD'])    
            if 'QUARTER' in params.granularity:
                start_quarter=last_month
                results=get_start_quarter(last_month)
                print(results)
                if results:
                    start_quarter=results[0][0]
                    quarter=results[1]
                    where='WHERE '+' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=[] and filter_sql.column!= 'PERIOD_GRANULARITY'])
                    where+=f''' and EV_VOLUMES_TEST.PERIOD_GRANULARITY = 'MONTH' and EXTRACT(YEAR FROM EV_VOLUMES_TEST.DATE)={last_year} and EXTRACT(MONTH FROM EV_VOLUMES_TEST.DATE) in {tuple(range(start_quarter,int(nb_months+1)))} ''' 
                    query_ytd=f'''
                    SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
                    FROM EV_VOLUMES_TEST
                    INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE {where}
                    GROUP BY  {columns_string}, EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY 
                    '''           
                    cur = conn.cursor()
                    cur.execute(query_ytd)
                    data_qtd = process_data(cur.fetch_pandas_all(),params.columns,['MONTH']).set_index(params.columns).sum(axis=1)
                    print(data_ytd)
                    dict_TD[f'{last_year}{quarter}QTD']=pd.DataFrame(data_qtd,columns=[f'{last_year}{quarter}QTD'])              

                
    # print(core_data) 
    ytd_dir= r'conf/ytd.sql'
    with open(ytd_dir, "r") as f:
        query_ytd=yaml.load(f)
    
    core_data_raw=process_data(core_data,params.columns,list(params.granularity))
    print(core_data_raw)
    core_data_raw.set_index(params.columns,inplace=True)
    print(dict_TD)
    core_data_raw.to_csv('coredata.csv')
    for key,df in dict_TD.items():
        df.to_csv(f'{key}.csv')
        print(pd.concat([core_data,df],axis=1),keys=params.columns)
    core_data_raw.reset_index(inplace=True)
    print(core_data)    
    core_data=build_df(core_data_raw,params.columns,list(params.graph_columns))        
    print(core_data)
    core_data.to_csv('test.csv')
    dataframe_list=[]
    if 'growth_seq' in params.metrics:
        growth_seq=core_data.copy()
        growth_seq=compute_growth_date_over_date(growth_seq,params.columns)
        # growth_YoY.to_csv('growth_YoY.csv')
        growth_seq['METRICS']='Sequential growth'
        dataframe_list+=[growth_seq]
    if 'growth_YoY' in params.metrics:
        growth_YoY=core_data.copy()
        growth_YoY=compute_growth_date_on_date(growth_YoY,params.columns)
        # growth_YoY.to_csv('growth_YoY.csv')
        growth_YoY['METRICS']='YoY growth'    
        dataframe_list+=[growth_YoY]            
    if 'mkt_share' in params.metrics:
        mkt_share=core_data.copy()
        mkt_share=compute_mkt_share(mkt_share,params.columns,params.granularity,conn)
        mkt_share['METRICS']='mkt_share'
        dataframe_list+=[mkt_share]            
    core_data['METRICS']='Absolute'
    dataframe_list+=[core_data]            
    
    complete_df=pd.concat(dataframe_list,axis=0)

    return complete_df.to_json(date_format="iso", orient="split")

@app.get("/quicktest", tags=["perfTest"])
async def get_data(api_key: APIKey = Depends(get_api_key)):
    query='''
    SELECT SUM(VALUE) FROM EV_VOLUMES_TEST WHERE BRAND='Tesla' and PERIOD_GRANULARITY='MONTH' and DATE='2020-12-31' GROUP BY BRAND
    '''
    core_data = await fetch_data(query)
    return core_data.to_json(date_format="iso", orient="split")

async def fetch_data(query):
    with open("conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    conn = snowflake.connector.connect(
        user=os.environ["USER_SF"],
        password=os.environ["PSW_SF"],
        account=os.environ["ACCOUNT_SF"],
        **parameters["snowflake_config"]) 
    cur = conn.cursor()
    cur.execute(query)
    core_data = cur.fetch_pandas_all()    
    return core_data