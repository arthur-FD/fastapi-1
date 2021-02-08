
import requests
import json 
import pandas as pd


# def get_token():
#     response = requests.post('http://127.0.0.1:8080/token',data	={'username':'arthur','password':'mypassword'})
#     return response.json()

def get_data(body):
    response = requests.post('http://127.0.0.1:8050/custom_view',data=body,headers = {'access_token': os.environ['API_KEY']})
    response=json.loads(response.json())
    return pd.DataFrame(response['data'], columns=response['columns'])

body={   "filters" : [
                {
                    "column": "OEM_GROUP",
                    "value_filter": [
                    "VW Group"
                    ]
                },

                {
                    "column": "PERIOD_GRANULARITY",
                    "value_filter": [
                    "YEAR","QUARTER"
                    ]
                }
                ],
    "columns" : ["OEM_GROUP","BRAND","PROPULSION"],
    "graph_columns": [],
    "metrics" : ["absolute","growth_YoY"],
    "granularity": ["YEAR","QUARTER"]
}

body=json.dumps(body)

result= get_data(body)
