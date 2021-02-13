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
from main import app,get_api_key,APIKey,API_params,filter_sql,sort_model


@app.post("/custom_view", tags=["custom query"])
def get_sort(params: API_params = Depends(sort_model)):
    params.columns
    # params.columns=cols_model_id(params.columns)
    YTD=False
    QTD=False
    with open("conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open("conf/mapping_columns.yml", "r") as file:
        mapping = yaml.load(file, Loader=ConfigLoader)
    columns_string=','.join(map(lambda x: f'{mapping[x]}.{x}' ,cols_model_id(params.columns.copy())))
    filters_list=params.filters
    filters_str=[mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(r"'MHEV',",r" 'MHEV 12V / 48V','MHEV 48V','MHEV 24V','MHEV 12V',").replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=tuple() and filter_sql.column!="DATE"]
    where=''
    if filters_str!=[]:
        where=' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(r"'MHEV',",r" 'MHEV 12V / 48V','MHEV 48V','MHEV 24V','MHEV 12V',").replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=tuple() and filter_sql.column!="DATE"])
    list_to_exclude=[]
    # if params.date_filter:
        
    #     start_date=params.date_filter['start_date']
    #     if params.metrics!=[]:
    #         start_date=f'{int(start_date[:4])-1}{start_date[4:]}'
    #         list_to_exclude=get_list_date(start_date,params.date_filter['start_date'],params.granularity)
            
    #     date_filtering=f"AND EV_VOLUMES_TEST.DATE>='{start_date}' AND EV_VOLUMES_TEST.DATE<='{params.date_filter['end_date']}'"
    date_filtering=''
    if params.date_filter:
        for gran,dict_date_filter in params.date_filter.items():
            if gran in params.granularity:
                if date_filtering=='':
                    date_filtering+=f"(EV_VOLUMES_TEST.PERIOD_GRANULARITY='{gran}' and "
                else: 
                    date_filtering+=f"OR (EV_VOLUMES_TEST.PERIOD_GRANULARITY='{gran}' and "
                start_date=dict_date_filter['start_date']
                if params.metrics!=[]:
                    start_date=f'{int(start_date[:4])-1}{start_date[4:]}'
                    list_to_exclude+=get_list_date(start_date,dict_date_filter['start_date'],[gran])
                date_filtering+=f"EV_VOLUMES_TEST.DATE>='{start_date}' AND EV_VOLUMES_TEST.DATE<='{dict_date_filter['end_date']}') "
    if date_filtering!='' and where!='':
        concat_filter=f'WHERE {date_filtering} AND ({where})'
    elif date_filtering!='' or where!='':
        concat_filter='WHERE ' + date_filtering+where        
    query=f'''
        SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
        FROM EV_VOLUMES_TEST
        INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE
        INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID {concat_filter}
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
    core_data= data_split_model_id(core_data,cols_model_id(params.columns.copy()).copy())
    last_date=date(1900,1,1)
    for gran in params.granularity:
        postential_last_date=core_data[core_data.PERIOD_GRANULARITY==gran].DATE.max()
        if postential_last_date>last_date:
            last_date=postential_last_date
            last_date_gran=gran
    last_date_display=display_date(last_date_gran,last_date)            
    dict_TD=dict()
    if 'YEAR' in params.granularity or 'QUARTER' in params.granularity:
        last_year=core_data[core_data.PERIOD_GRANULARITY=='YEAR'].DATE.max().year
        last_month=core_data[core_data.PERIOD_GRANULARITY=='YEAR'].DATE.max().month
        query_month_availables=f'''select MONTHS_AVAILABLE FROM CONFIG where YEAR={last_year}'''
        cur = conn.cursor()
        cur.execute(query_month_availables)
        nb_months = cur.fetch_pandas_all()['MONTHS_AVAILABLE'].iloc[0]        
        if 0<nb_months<=12:     
            if 'YEAR' in params.granularity:
                print(filters_list)
                where= ' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(r"'MHEV',",r" 'MHEV 12V / 48V','MHEV 48V','MHEV 24V','MHEV 12V',").replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter != tuple() and filter_sql.column!= 'PERIOD_GRANULARITY'])
                date_filtering=f''' EV_VOLUMES_TEST.PERIOD_GRANULARITY = 'MONTH' and EXTRACT(YEAR FROM EV_VOLUMES_TEST.DATE)={last_year-1} and EXTRACT(MONTH FROM EV_VOLUMES_TEST.DATE) in {tuple(range(0,int(nb_months+1)))} ''' 
                if date_filtering!='' and where!='':
                    concat_filter=f'WHERE {date_filtering} AND ({where})'
                elif date_filtering!='' or where!='':
                    concat_filter='WHERE ' + date_filtering+where                   
                query_ytd=f'''
                SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
                FROM EV_VOLUMES_TEST
                INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE
                INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID {concat_filter}
                GROUP BY  {columns_string}, EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY 
                '''   
                cur = conn.cursor()
                cur.execute(query_ytd)
                data_ytd = cur.fetch_pandas_all()
                data_ytd= data_split_model_id(data_ytd,cols_model_id(params.columns.copy())) 
                try:
                    data_ytd = process_data(data_ytd,params.columns,['MONTH']).set_index(params.columns).sum(axis=1)
                    dict_TD[f'{last_year-1}YTD']=pd.DataFrame(data_ytd,columns=[f'{last_year-1}YTD'])                    
                except:
                    data_ytd=core_data.set_index(params.columns).sum(axis=1)*0
                    dict_TD[f'{last_year-1}YTD']=pd.DataFrame(data_ytd,columns=[f'{last_year-1}YTD'])
                YTD=True
            if 'QUARTER' in params.granularity:
                start_quarter=last_month
                results=get_start_quarter(last_month)
                if results:
                    start_quarter=results[0][0]
                    quarter=results[1]
                    where='WHERE '+' AND '.join( [mapping[filter_sql.column]+'.'+filter_sql.column+' IN '+str(filter_sql.value_filter).replace(r"'MHEV',",r" 'MHEV 12V / 48V','MHEV 48V','MHEV 24V','MHEV 12V',").replace(',)',')') for filter_sql in filters_list if filter_sql.value_filter !=[] and filter_sql.column!= 'PERIOD_GRANULARITY'])
                    where+=f''' and EV_VOLUMES_TEST.PERIOD_GRANULARITY = 'MONTH' and EXTRACT(YEAR FROM EV_VOLUMES_TEST.DATE)={last_year-1} and EXTRACT(MONTH FROM EV_VOLUMES_TEST.DATE) in {tuple(range(start_quarter,int(nb_months+1)))} ''' 
                    if date_filtering!='' and where!='':
                        concat_filter=f'WHERE {date_filtering} AND ({where})'
                    elif date_filtering!='' or where!='':
                        concat_filter='WHERE ' + date_filtering+where  
                    query_ytd=f'''
                    SELECT {columns_string},EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE)
                    FROM EV_VOLUMES_TEST
                    INNER JOIN GEO_COUNTRY_TEST ON EV_VOLUMES_TEST.SALES_COUNTRY_CODE=GEO_COUNTRY_TEST.COUNTRY_CODE
                    INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID {where}
                    GROUP BY  {columns_string}, EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY 
                    '''           
                    cur = conn.cursor()
                    cur.execute(query_ytd)
                    data_qtd = cur.fetch_pandas_all()
                    data_qtd= data_split_model_id(data_qtd,cols_model_id(params.columns.copy())) 
                    try:
                        data_qtd = process_data(data_qtd,params.columns,['MONTH']).set_index(params.columns).sum(axis=1)
                        dict_TD[f'{last_year-1}{quarter}QTD']=pd.DataFrame(data_qtd,columns=[f'{last_year-1}{quarter}QTD'])                  
                    except:
                        data_qtd=core_data.set_index(params.columns).sum(axis=1)*0
                        dict_TD[f'{last_year-1}{quarter}QTD']=pd.DataFrame(data_qtd,columns=[f'{last_year-1}{quarter}QTD'])                    
                    QTD=True
    core_data_raw=process_data(core_data,params.columns,list(params.granularity))  
    core_data_raw.set_index(params.columns,inplace=True)
    if dict_TD!=dict():
        for key,df in dict_TD.items():
            col_last_period=last_period(key)
            core_data_raw=pd.concat([df,core_data_raw],axis=1)
            core_data_raw.columns=[col if col!=col_last_period else f'{col}{key[-3:]}' for col in core_data_raw.columns]
    cols_other=list(core_data_raw.columns)            
    core_data_raw.reset_index(inplace=True)
    core_data_raw.columns=params.columns+cols_other
    core_data=build_df(core_data_raw,params.columns.copy(),list(params.graph_columns))   
    if YTD and last_date_gran=='YEAR': last_date_display=last_date_display+'YTD'
    elif QTD and last_date_gran=='QUARTER': last_date_display=last_date_display+'YTD'
    core_data.sort_values(by=last_date_display,ascending=[False],inplace=True)
    list_sort=core_data[core_data[params.columns[0]]!='Total'][params.columns[0]].unique()
    sorterIndex = dict(zip(list_sort, range(len(list_sort))))
    sorterIndex['Total']=-1
    print(sorterIndex)   
    dataframe_list=[]
    if 'growth_seq' in params.metrics:
        growth_seq=core_data.copy()
        growth_seq=compute_growth_date_over_date(growth_seq,params.columns.copy(),params.graph_columns)
        growth_seq['METRICS']='Sequential growth'
        dataframe_list+=[growth_seq]
    if 'growth_YoY' in params.metrics:
        growth_YoY=core_data.copy()
        growth_YoY=compute_growth_date_on_date(growth_YoY,params.columns.copy(),params.graph_columns)
        growth_YoY['METRICS']='YoY growth'   
        dataframe_list+=[growth_YoY]            
    if 'mkt_share' in params.metrics:
        mkt_share=core_data.copy()
        body=get_default_body(name='mkt_share')
        response=get_sort(body)   
        response=json.loads(response)
        data_prop=pd.DataFrame(response['data'],columns=response['columns'])
        mkt_share=compute_mkt_share(mkt_share,params.columns.copy(),params.granularity,conn,data_prop.copy(),params.graph_columns)
        mkt_share['METRICS']='mkt_share'
        dataframe_list+=[mkt_share]                     
    if 'mkt_share_growth' in params.metrics:
        if  'mkt_share' not in params.metrics:
            mkt_share=core_data.copy()
            body=get_default_body(name='mkt_share')
            response=get_sort(body)   
            response=json.loads(response)
            data_prop=pd.DataFrame(response['data'],columns=response['columns'])
            mkt_share=compute_mkt_share(mkt_share,params.columns.copy(),params.granularity,conn,data_prop.copy(),params.graph_columns)
            mkt_share['METRICS']='mkt_share'
        growth_mkt_share=mkt_share.drop(['METRICS'],axis=1)
        growth_mkt_share=compute_growth_date_on_date(growth_mkt_share,params.columns.copy(),params.graph_columns)
        growth_mkt_share['METRICS']='mkt_share_growth'
        dataframe_list+=[growth_mkt_share]
        
    core_data['METRICS']='Absolute'
    dataframe_list+=[core_data]            
    complete_df=pd.concat(dataframe_list,axis=0)
    if list_to_exclude!=[]:
        complete_df=complete_df[[col for col in complete_df.columns if not col in list_to_exclude]]
    complete_df['SORT']=complete_df[params.columns[0]].map(sorterIndex)
    complete_df.sort_values(by=['METRICS','SORT'],ascending = [True,True],inplace=True)
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


@app.get("/get_filter_list", tags=["filters"])
def get_filters(api_key: APIKey = Depends(get_api_key)):
    with open("conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open("conf/filter_list.yml", "r") as file:
        filter_list = yaml.load(file, Loader=ConfigLoader)
    conn = snowflake.connector.connect(
    user=os.environ["USER_SF"],
    password=os.environ["PSW_SF"],
    account=os.environ["ACCOUNT_SF"],
    **parameters["snowflake_config"]
    )  
    filter_list_result={}
    for filter_col, filter_info in filter_list.items():
        cur = conn.cursor()
        cur.execute(filter_info['query'])
        data = cur.fetch_pandas_all()
        filter_list_result[filter_col]=sorted(list(set(list(data[data.columns[0]].apply(filter_funcs(filter_info['func'])).values))))
    return filter_list_result