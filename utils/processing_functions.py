import os
from datetime import date
import json
import pycountry
from datetime import datetime
import numpy as np
import re
import calendar
import pandas as pd
from statistics import mean
from utils.utils import *
import yaml
import warnings
warnings.filterwarnings("ignore")
import snowflake.connector

columns_display_dir= r'./conf/columns_display.yml'
with open(columns_display_dir, "r") as f:
    columns_display=yaml.load(f)

def process_data(data,columns,granularity):
    if 'PROPULSION'  in columns:
        data.PROPULSION=data.PROPULSION.apply(lambda propulsion_string:'MHEV' if 'MHEV' in propulsion_string else propulsion_string)
    dict_data_granularity={}
    dict_data_granularity_pct_prop={}
    for gran in granularity:
        dict_data_granularity[gran]=data[data['PERIOD_GRANULARITY']==gran]
        # if start_date[gran]:
        #     dict_data_granularity[gran]=dict_data_granularity[gran][dict_data_granularity[gran].DATE >= datetime.strptime(start_date[gran],r'%Y-%m-%d').date()]
        # if end_date[gran]:
        #     dict_data_granularity[gran]=dict_data_granularity[gran][dict_data_granularity[gran].DATE <= datetime.strptime(end_date[gran],r'%Y-%m-%d').date()]
        dict_data_granularity[gran].drop(['PERIOD_GRANULARITY'],inplace=True,axis=1)
        list_col_ordered=list(map(lambda date_obj:display_date(gran,date_obj),sorted(list(set(dict_data_granularity[gran].DATE)))))
        dict_data_granularity[gran]['DATE']=dict_data_granularity[gran].DATE.apply(lambda date_obj:display_date(gran,date_obj))
        dict_data_granularity[gran]=dict_data_granularity[gran].groupby( columns+['DATE']).sum()
        if columns!=[]:
            dict_data_granularity[gran]=dict_data_granularity[gran].unstack(level='DATE')
            dict_data_granularity[gran].columns=[col[-1] for col in list(dict_data_granularity[gran].columns)]
        else: 
            dict_data_granularity[gran]=dict_data_granularity[gran].transpose()
        dict_data_granularity[gran]=dict_data_granularity[gran].replace(np.nan,0.0)[list_col_ordered]
    print(dict_data_granularity)
    data_Filtered=pd.concat([df for df in dict_data_granularity.values()],axis=1)
    data_Filtered.reset_index(inplace=True)
    # if bool_YTD_year:
    #     data_Filtered.columns=[str(last_date.year )+'YTD'  if col ==str(last_date.year ) else col for col in data_Filtered.columns]
    # if bool_YTD_quarter:
    #     data_Filtered.columns=[str(last_date.year )+'Q'+str(quarter)+'QTD' if col ==str(last_date.year )+'Q'+str(quarter) else col for col in data_Filtered.columns ]
    if 'SALES_COUNTRY_CODE' in columns:
        data_Filtered['SALES_COUNTRY_CODE']=data_Filtered['SALES_COUNTRY_CODE'].apply(lambda country_code:pycountry.countries.get(alpha_2=country_code).name )          
    return data_Filtered

def compute_mkt_share(mkt_share,columns,granularity,conn):
        cols_mkt_share=list(set(mkt_share.columns)-set(columns))
        if 'PROPULSION' in columns:
            query=f'''SELECT EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE) 
            FROM EV_VOLUMES_TEST 
            where  EV_VOLUMES_TEST.PERIOD_GRANULARITY in {str(granularity).replace(',)',')')}
            GROUP BY EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY '''
            cur = conn.cursor()
            cur.execute(query)
            data_prop = cur.fetch_pandas_all()
            data_prop=process_data(data_prop,['PROPULSION'],granularity)
            # print('cc')
            # cols_prop=list(set(data_prop.columns)-set(['PROPULSION']))
            # print(cols_prop)
            for prop in mkt_share.PROPULSION.unique():
                if prop!='':
                    mkt_share.loc[mkt_share.PROPULSION==prop,cols_mkt_share]=mkt_share.loc[mkt_share.PROPULSION==prop,cols_mkt_share].div(data_prop.loc[data_prop.PROPULSION==prop,cols_mkt_share].iloc[0],axis=1)*100
            mkt_share.loc[mkt_share.PROPULSION=='',cols_mkt_share]=mkt_share.loc[mkt_share.PROPULSION=='',cols_mkt_share].div(data_prop[cols_mkt_share].sum(),axis=1)*100
        else:
            query=f'''SELECT EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,sum(EV_VOLUMES_TEST.VALUE) 
            FROM EV_VOLUMES_TEST 
            where  EV_VOLUMES_TEST.PERIOD_GRANULARITY in {str(granularity).replace(',)',')')}
            GROUP BY EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.DATE,EV_VOLUMES_TEST.PERIOD_GRANULARITY '''
            cur = conn.cursor()
            cur.execute(query)
            data_sum = cur.fetch_pandas_all()
            data_sum=process_data(data_sum,[],granularity)
            mkt_share[cols_mkt_share]=mkt_share[cols_mkt_share].div(data_sum[cols_mkt_share].iloc[0],axis=1)*100
        return mkt_share


def build_df(data,columns,graph_columns=[]):
    data_processed=pd.DataFrame(columns=data.columns)
    for i in range(min(3,len(columns))):
        temp=data.groupby(columns[:i+1]).sum()
        temp.reset_index(inplace=True)
        if columns[i] in graph_columns:
            temp['graph']='true'
        data_processed=pd.concat([data_processed,temp],axis=0)
    if len(columns)>=4:
        data_processed=pd.concat([data_processed,data],axis=0)
    if len(data_processed)>1:        
        Total_sum=pd.DataFrame(data.set_index(columns).sum()).transpose()        
        Total_sum[columns[0]]='Total'
        data_processed=pd.concat([data_processed,Total_sum],axis=0)
    if graph_columns!=[]:
        data_processed=data_processed[['graph']+list(data_processed.columns)[:-1]]    
    return data_processed.fillna('')

# def compute_growth(data,columns):
#     col_tab_date={'MONTH':pd.DataFrame(),'QUARTER':pd.DataFrame(),'YEAR':pd.DataFrame(),'QUARTER_QTD':pd.DataFrame(),'YEAR_YTD':pd.DataFrame()}
#     final_growth=pd.DataFrame()
#     data=data.set_index(columns)
#     for col in list(set(data.columns)-set(columns)):
#         gran=check_date_gran(col)
#         if gran:
#             col_tab_date[gran]=pd.concat([col_tab_date[gran],data[[col]]],axis=1)
#             col_tab_date[gran]=col_tab_date[gran][col_tab_date[gran].columns.sort_values()]
#     for gran,df in col_tab_date.items():
#         if not df.equals(pd.DataFrame()):
#             df=df.pct_change(axis='columns')
#             final_growth=pd.concat([final_growth,df],axis=1)
#     # final_growth=final_growth*data
#     #  final_growth=final_growth.applymap("{0:.2%}".format)
#     # final_growth.reset_index(inplace=True)
#     # final_growth=final_growth.replace(f'{str(np.nan)}%','-')
#     # final_growth=final_growth.replace(f'{str(np.inf)}%','-')
#     # final_growth.columns=[col+'_pct' for col in final_growth.columns]
#     # concat_growth=pd.concat([final_growth,data],axis=1)
#     return final_growth,concat_growth

def compute_growth_date_over_date(data,columns):
    col_tab_date={'MONTH':[],'QUARTER':[],'YEAR':[],'QUARTER_QTD':[],'YEAR_YTD':[]}
    data=data.set_index(columns)
    final_growth=pd.DataFrame()
    for col in data.columns:
        col_tab_date[check_date_gran(col)].append(col)
    for gran, cols in col_tab_date.items():
        if cols !=[]:
            final_growth=pd.concat([final_growth,data[sort_display_date(cols,gran)].pct_change(axis='columns')],axis=1)
    final_growth.reset_index(inplace=True)
    return final_growth

# def compute_mkt_share(data,columns):
#     if 'PROPULSION' in data.columns:
#         mkt_share=data[data['PROPULSION']==''].set_index(columns).astype(float).divide(data[data['REGION']=='Total'].set_index(columns).astype(float).iloc[0]).reset_index()
#         sum_prop=data[data['PROPULSION']!=''].groupby('PROPULSION').sum()
#         for prop in sum_prop.index:
#             mkt_share=pd.concat([mkt_share,data[data['PROPULSION']==prop].set_index(columns).astype(float).divide(sum_prop.loc[prop]).reset_index()],axis=0,ignore_index=True)
#     return mkt_share.set_index(columns).applymap(lambda x: format_string(x,type_data='pct')).reset_index()

def compute_growth_date_on_date(data,columns):
    col_tab_date={'MONTH':{},'QUARTER':{'Q1':pd.DataFrame(),'Q2':pd.DataFrame(),'Q3':pd.DataFrame(),'Q4':pd.DataFrame()},'YEAR':pd.DataFrame(),'QUARTER_QTD':pd.DataFrame(),'YEAR_YTD':pd.DataFrame()}
    data=data.set_index(columns)

    final_growth=pd.DataFrame()
    for col in data.columns:
        if check_date_gran(col)=='QUARTER':
            col_tab_date['QUARTER'][col[-2:]]=pd.concat([col_tab_date['QUARTER'][col[-2:]],data[[col]]],axis=1)
        elif check_date_gran(col)=='MONTH':
            if col[:3] not in col_tab_date['MONTH'].keys():
                col_tab_date['MONTH'][col[:3]]=pd.DataFrame()
            col_tab_date['MONTH'][col[:3]]=pd.concat([col_tab_date['MONTH'][col[:3]],data[[col]]],axis=1)
        else:
            col_tab_date[check_date_gran(col)]=pd.concat([col_tab_date[check_date_gran(col)],data[[col]]],axis=1)
    for gran, elt in col_tab_date.items():
        if type(elt)==type(dict()):
            if elt != {}:
                cols=[]
                for date_gran, df in elt.items():
                    cols_temp=list(df.columns)
                    final_growth=pd.concat([final_growth,df[sort_display_date(cols_temp,gran)].pct_change(axis='columns')],axis=1)
                    cols+=cols_temp
        elif not elt.equals(pd.DataFrame()):
            cols=list(elt.columns)
            final_growth=pd.concat([final_growth,elt[sort_display_date(cols,gran)].pct_change(axis='columns')],axis=1)
    final_growth.reset_index(inplace=True)
    return final_growth


def compute_growth(data,columns,metrics):
    if metrics=='date_over_date':
        return compute_growth_date_over_date(data,columns)
    elif metrics== 'date_on_date':
        return compute_growth_date_on_date(data,columns)

def format_string(num,type_data='numerical'):
    if type_data=='pct':
        if num>=0:
            return "{0:,.2%}".format(num)
        elif num==np.nan or num==np.inf:
            return '-'
        else: 
            return f'<span style="color:red;">({r"{:,}".format(round(abs(num)*100,2))})%</span>'
    if type_data=='numerical':
        try:
            return "{0:,.0f}".format(num)
        except:
            return num


def build_columns(data,columns,graph_columns=None):
    mapping_snowflakes_display_dir=r'./conf/mapping_snowflakes_display.yml'
    with open(mapping_snowflakes_display_dir, "r") as f:
        mapping_snowflakes_display=yaml.load(f)
    mapping_snowflakes_display_width_dir=  r'./conf/mapping_snowflakes_display_width.yml'
    with open(mapping_snowflakes_display_width_dir, "r") as f:
        mapping_snowflakes_display_width=yaml.load(f)        
    col_tree=columns[:min(len(columns),4)]
    col_outside=list(set(columns)-set(col_tree))
    gran_subtable_dict={'QUARTER':'Quarterly','MONTH': 'Monthly','YEAR': 'Annual','QUARTER_QTD':'Quarter To Date','YEAR_YTD':'Year To Date'}
    col_tab_date={'MONTH':[],'QUARTER':[],'YEAR':[],'QUARTER_QTD':[],'YEAR_YTD':[]}
    col_tab_other=[]
    ns = Namespace("myNamespace", "tabulator")

    for col in list(data.columns):
        if check_date_gran(col)=="MONTH":
            # col_tab_date[check_date_gran(col)].append({'title':col.title(),'field':col,"formatter":"html", "width":mapping_snowflakes_display_width[check_date_gran(col)],"topCalc":ns("freez")})
            col_tab_date[check_date_gran(col)].append({'title':col.title(),'field':col,"formatter":"html", "width":mapping_snowflakes_display_width[check_date_gran(col)]})
        elif check_date_gran(col):
            # col_tab_date[check_date_gran(col)].append({'title':col,'field':col,"formatter":"html", "width":mapping_snowflakes_display_width[check_date_gran(col)],"topCalc":ns("freez")})
            col_tab_date[check_date_gran(col)].append({'title':col,'field':col,"formatter":"html", "width":mapping_snowflakes_display_width[check_date_gran(col)],})
            
        elif col in [col_tree[0]]+col_outside:
            # col_tab_other.append({'title':mapping_snowflakes_display[col],'field':col,"frozen":'true', "width":"210","headerFilter": "input","headerFilterFunc": ns("deepMatchHeaderFilter"),"headerFilterFuncParams":{"columnName": col}})
            # col_tab_other.append({'title':mapping_snowflakes_display[col],'field':col,"frozen":'true', "width":mapping_snowflakes_display_width[col],"headerFilter": "input","topCalc":ns("freez")})
            col_tab_other.append({'title':mapping_snowflakes_display[col],'field':col,"frozen":'true', "width":mapping_snowflakes_display_width[col],"headerFilter": "input"})
            #"headerFilter": "input","headerFilterFunc": nsdeepMatchHeaderFilter, "headerFilterFuncParams": {"columnName":col,}
    col_tab=col_tab_other+[{'title':'metrics','field':'METRICS',"frozen":'true','visible':False}]
    if graph_columns:
        col_tab=[{"title": "", "field": "graph","editor":"true", "hozAlign": "center",  "formatter":"tickCross", "formatterParams":{"tickElement":"<i class='fa fa-chart-line'></i>","crossElement":"",},"frozen":"true", "headerSort":"false",}]+col_tab

    for gran,list_col in col_tab_date.items():
        if list_col!=[]:
            # list_col[-1]['dir']="desc"
            col_tab+=[{
                'title':gran_subtable_dict[gran],
                'columns':list_col,   
        }]
    return col_tab
