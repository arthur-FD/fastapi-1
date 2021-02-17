# import sys
# sys.path.append('../')
from fastapi.testclient import TestClient
import os
from xev_api.main import API_KEY_NAME,API_KEY
from xev_api.sf_connector import app
import json
import re

client = TestClient(app)

def test_filters():
    with open("test/test_filter_result.json", encoding='utf-8') as file:
        test_filter_result = json.load(file)
    response = client.get("/get_filter_list/?ingestion_date=2021-02-06", headers={API_KEY_NAME: API_KEY})
    assert response.status_code == 200
    assert response.json() ==test_filter_result

def test_get_ingestion():
    response = client.get("/get_last_ingestion_date", headers={API_KEY_NAME: API_KEY})
    ingestion_date=response.json()
    pattern=r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    assert response.status_code == 200
    assert re.search(pattern, ingestion_date)

def test_get_custom():
    with open("test/test_custom_query_config.json", encoding='utf-8') as file:
        body = json.load(file)    
    response = client.post("/custom_view", json=body,headers={API_KEY_NAME: API_KEY})
    assert response.status_code == 200
