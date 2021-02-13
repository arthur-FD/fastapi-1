
import requests
import json 
import pandas as pd
import os

# def get_token():
#     response = requests.post('http://127.0.0.1:8080/token',data	={'username':'arthur','password':'mypassword'})
#     return response.json()

def get_data(body):
    response = requests.post('http://127.0.0.1:8050/custom_view',data=body,headers = {'access_token': os.environ['API_KEY']})
    print(response)
    response=json.loads(response.json())
    return pd.DataFrame(response['data'], columns=response['columns'])

body={
    "filters": [
        {
            "column": "PERIOD_GRANULARITY",
            "value_filter": [
                "YEAR"
            ]
        }
    ],
    "columns": [
        "PROPULSION",
        "REGION",
        "SALES_COUNTRY_CODE",
        "CATHODE"
    ],
    "graph_columns": ["SALES_COUNTRY_CODE"],
    "metrics": [
        "absolute",
        "growth_YoY",
        "growth_seq",
        "mkt_share"
    ],



    "granularity": [
        "YEAR"
    ],
    "date_filter": {
        "YEAR": {
            "start_date": "2015-12-31",
            "end_date": "2020-12-31"
        }
    }
}

body={
    "filters": [
        {
            "column": "PERIOD_GRANULARITY",
            "value_filter": [
                "YEAR","QUARTER","MONTH"
            ]
        }
    ],
    "columns": [
        "PROPULSION"
    ],
    "graph_columns": [],
    "metrics": [
        "absolute",
    ],
    
    "granularity": [
         "YEAR","QUARTER","MONTH"
    ],
    "date_filter": {

    }
}

body=json.dumps(body)

result= get_data(body)


ex={"filters": [{"column": "REGION", "value_filter": ["Europe", "Asia-Pacific", "Americas", "Africa & ME"]}, {"column": "SALES_COUNTRY_CODE", "value_filter": ["CL", "GB", "GE", "MC", "KZ", "NZ", "KW", "PT", "EE", "PA", "MY", "MU", "IL", "BG", "LI", "NL", "TR", "US", "OM", "EG", "PL", "ID", "DZ", "SA", "MT", "CR", "HR", "AZ", "SI", "MA", "QA", "UA", "TW", "CA", "KR", "PF", "JO", "MK", "RO", "LB", "IS", "PE", "MO", "SE", "CO", "AD", "HU", "CH", "IQ", "AT", "PR", "IE", "BR", "BA", "UY", "DE", "BH", "IN", "NO", "EC", "IT", "BE", "MX", "TN", "HK", "GR", "LU", "AU", "FR", "AE", "BS", "MD", "SK", "ZA", "JP", "LK", "FI", "RE", "MG", "CY", "LT", "SG", "DK", "CN", "NP", "TH", "PH", "BM", "LV", "RW", "VN", "FJ", "RS", "BY", "AR", "CZ", "SM", "RU", "ES"]}, {"column": "PROPULSION", "value_filter": ["", "BEV", "HEV", "MHEV", "PHEV", "FCEV"]}], "columns": ["PROPULSION", "REGION", "SALES_COUNTRY_CODE"], "graph_columns": [], "metrics": ["growth_YoY", "growth_seq", "mkt_share"], "granularity": ["YEAR"], "date_filter": {"YEAR": {"start_date": "2015-12-31", "end_date": "2020-12-31"}, "QUARTER": {"start_date": "2015-12-31", "end_date": "2020-12-31"}, "MONTH": {"start_date": "2015-12-31", "end_date": "2020-12-01"}}}

body=json.dumps(ex)

result= get_data(body)

def get_filter():
    response = requests.get('http://127.0.0.1:8050/get_filter_list',headers = {'access_token': os.environ['API_KEY']})
    # response=json.loads(response.json())
    return response.json()



body_graph_prop={
    "filters": [
        {
            "column": "PERIOD_GRANULARITY",
            "value_filter": [
                "YEAR"
            ]
        }
    ],
    "columns": [
        "PROPULSION"
    ],
    "graph_columns": ["REGION"],
    "metrics": [
        "absolute",
        "mkt_share"
    ],



    "granularity": [
        "YEAR"
    ],
    "date_filter": {
        "YEAR": {
            "start_date": "2008-12-31",
            "end_date": "2020-12-31"
        }
    }
}

body=json.dumps(body_graph_prop)

result= get_data(body)
