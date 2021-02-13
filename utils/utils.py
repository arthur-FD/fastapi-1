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
from datetime import datetime, timedelta
from collections import OrderedDict
from main import APIKey,API_params,filter_sql,sort_model

def get_id_from_child(list_child):
    return [children['props']['id'] for children in list_child if children['props']['hidden']==False]

def format_growth(num):
    if type(num)==type(''):
        return num
    else:
        return '{:.2%}'.format(num) if num>0 else'({:.2%})'.format(abs(num))

def display_date(granularity,date):
    quarter_dict={'3':'Q1','6':'Q2','9':'Q3','12':'Q4'}
    if granularity=='YEAR':
        return str(date.year)
    if granularity == 'MONTH':
        return f'{calendar.month_name[date.month].lower()[:3]}-{str(date.year)[2:]}'
    if granularity == 'QUARTER':
        if str(date.month) in quarter_dict.keys() and date.year>2012:
            return f'{str(date.year)}{quarter_dict[str(date.month)]}'
    if granularity == 'QUARTER_QTD':
        if str(date.month) in quarter_dict.keys() and date.year>2012:
            return f'{str(date.year)}{quarter_dict[str(date.month)]}QTD'            
    if granularity == 'YEAR_YTD':
        return f'{str(date.year)}YTD'              
    else: 
        return 'NO MATCHING GRANULARITY'

def check_date_gran(string_to_check):
        regex_year = r'^20[0-9]{2}$'
        regex_month = r'^[A-z]{3}-[0-9]{2}$'
        regex_quarter=r'^[0-9]{4}Q[1-4]{1}$'
        if re.search(regex_year,string_to_check):
            return 'YEAR'
        elif re.search(regex_month,string_to_check):
            return 'MONTH'
        elif re.search(regex_quarter,string_to_check):
            return 'QUARTER'
        elif 'YTD' in  string_to_check or 'QTD' in  string_to_check:
            if check_date_gran(string_to_check[:-3])=='QUARTER':
                return 'QUARTER_QTD'
            else: return 'YEAR_YTD'
        else: return None

def average_numbers_in_string(string_to_average):
    regex_pattern=r'[0-9]+,{0,1}[0-9]*'
    list_float=list(map(lambda x: float(x.replace(',','.')),re.findall(regex_pattern,string_to_average)))
    if len(list_float)>0:
        return mean(list_float)
    else:
        return 0


def numberOfDays(y, m):
      leap = 0
      if y% 400 == 0:
         leap = 1
      elif y % 100 == 0:
         leap = 0
      elif y% 4 == 0:
         leap = 1
      if m==2:
         return 28 + leap
      list = [1,3,5,7,8,10,12]
      if m in list:
         return 31
      return 30

def get_YTD_months(gran,last_date):
    # if gran =='YEAR' and last_date.month==12:
    #     return ''
    # elif gran == 'YEAR' and last_date.month!=12:
    if gran == 'YEAR':    
        start_date=date(last_date.year-1,1,31)
        end_date=date(last_date.year-1,last_date.month,last_date.day)
        return start_date, end_date
    elif gran == 'QUARTER' and last_date.month in [3,6,9,12]:
        return''
    elif gran == 'QUARTER' and last_date.month not in [3,6,9,12]:
        pos=sorted([3,6,9,12,last_date.month])
        start_date=date(last_date.year-1,pos[pos.index(last_date.month)-1]+1,numberOfDays(last_date.year,pos[pos.index(last_date.month)-1]))
        end_date=date(last_date.year-1,last_date.month,last_date.day)
        return start_date, end_date,pos.index(last_date.month)+1
    else: return ''

def generation_sorting_list(full_data):
    full_col_list= []

    for gran in ['YEAR','QUARTER','MONTH']:
        temp=list(full_data[full_data.PERIOD_GRANULARITY==gran].sort_values('DATE',ascending=False)['DATE'].apply(lambda date_to_display:display_date(gran,date_to_display)).unique())
        temp=[temp[0]+'YTD']+temp
        full_col_list+=temp

    return [{"column": col,'dir':'desc'} for col in full_col_list]

def get_date(date_string):
    quarter_dict={'Q1':3,'Q2':6,'Q3':9,'Q4':12}
    gran=check_date_gran(date_string)
    if gran=='MONTH': return datetime.strptime(date_string.lower(),r'%b-%y')
    elif gran=='YEAR': return date(int(date_string),12,31)
    elif gran== 'QUARTER':return date(int(date_string[:4]),quarter_dict[date_string[4:]],1)
    elif gran== 'QUARTER_QTD':return date(int(date_string[:4]),quarter_dict[date_string[4:6]],1)
    elif gran== 'YEAR_YTD':return date(int(date_string[:4]),12,1)

def sort_display_date(dates,gran):
    return list(map(lambda date_: display_date(gran,date_),sorted(map(get_date,dates))))

def get_start_quarter(month):
    quarter_dict={'3':'Q1','6':'Q2','9':'Q3','12':'Q4'}
    list_quarter=list(map(int,quarter_dict.keys()))
    if month in list_quarter:
        # return None
        return None
    else:
        list_quarter=[0]+list_quarter
        list_quarter_n_month=sorted(list_quarter+[month])
        idx_month=list_quarter_n_month.index(month)
        return([list_quarter_n_month[idx_month-1],list_quarter_n_month[idx_month+1]],quarter_dict[str(list_quarter[idx_month])])
    
def last_period(key):
    if 'Q' in key:
        return f'{int(key[:4])+1}{key[4:6]}'
    else:
        return str(int(key[:4])+1)
    
def get_list_date(date1,date2,gran_list):
    list_date_to_exclude=[]
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in [date1,date2]]
    for gran in gran_list:
        list_date_to_exclude+=OrderedDict((display_date(gran,(start + timedelta(_))), None) for _ in range((end - start).days)).keys()
        list_date_to_exclude=list_date_to_exclude[:-1]
    return list_date_to_exclude

def cols_model_id(cols):
    if 'MODEL' in cols or 'BATTERY'in cols or 'SUPPLIERS' in cols:
        cols+=['MODEL_ID']
        cols+=['PROPULSION']
        cols=list(set(cols)-set(['MODEL','BATTERY','SUPPLIERS']))
    return cols

def data_split_model_id(data,cols):
    if 'MODEL_ID' in cols:
        data['BATTERY']=data.MODEL_ID.apply(lambda model_id:model_id.split('||')[-1])
        data['MODEL']=data.MODEL_ID.apply(lambda model_id:model_id.split('||')[0])        
        suppliers_hev=data[data.PROPULSION.isin(['HEV','MHEV'])]['MODEL_ID'].apply(lambda model_id:model_id.split('||')[-2] if len(model_id.split('||'))>1 else '')   
        suppliers_bev=data[data.PROPULSION.isin(['BEV','PHEV','FCEV'])]['MODEL_ID'].apply(lambda model_id:model_id.split('||')[-3] if len(model_id.split('||'))>1 else '')   
        suppliers=pd.concat([suppliers_bev,suppliers_hev],axis=0)
        data['SUPPLIERS']=suppliers.sort_index()
    data['DATE']=data.DATE.apply(lambda str_date:datetime.strptime(str(str_date)[:10], r'%Y-%m-%d').date())
    data=data.replace(np.nan,'')
    return data

def filter_funcs(name):
    if name=='':
        return lambda x: x
    elif name=='propulsion':
        return lambda propulsion_string:'MHEV' if 'MHEV' in propulsion_string else propulsion_string
    elif name=='country_code':
        return lambda country_code:(pycountry.countries.get(alpha_2=country_code).name,country_code)
        
        

def get_default_body(name='mkt_share'):
    with open(f'json_config/{name}.json', "r") as f:
        body=json.loads(f.read())
    body=sort_model (filters=body['filters'],
                columns=body['columns'],
                graph_columns=body['graph_columns'],
                metrics=body['metrics'],
                granularity=body['granularity'],
                date_filter=body['date_filter'],
                )  
    return body