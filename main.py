from fastapi import FastAPI
import os
import pandas as pd
import snowflake.connector
import yaml
from utils.config_loader import ConfigLoader
import pathlib
app = FastAPI()

#domain where this api is hosted for example : localhost:5000/docs to see swagger documentation automagically generated.


@app.get("/")
def home():
    return {"message":"test main"}


@app.get("/region_view")
def get_region_view():
    with open(pathlib.Path(__file__).parent / "conf/parameter.yml", "r") as file:
        parameters = yaml.load(file, Loader=ConfigLoader)
    with open(pathlib.Path(__file__).parent / "conf/funct_query.sql", "r") as file:
        core_query = file.read()
        
    conn = snowflake.connector.connect(
        user=os.environ["USER_SF"],
        password=os.environ["PSW_SF"],
        account=os.environ["ACCOUNT_SF"],
        **parameters["snowflake_config"]
    ) 
    cur = conn.cursor()
    cur.execute(core_query)
    core_data = cur.fetch_pandas_all()
    return core_data.to_json(date_format="iso", orient="split")
