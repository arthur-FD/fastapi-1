from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import os
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse
from pydantic import BaseModel, create_model,constr
from typing import Optional,List,Dict,Tuple,Sequence
from fastapi import FastAPI, Depends, status, Query,Body


API_KEY = os.environ['API_KEY']
API_KEY_NAME = "access_token"
COOKIE_DOMAIN = "localtest.me"

api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

app = FastAPI(root_path='/xev_api')


async def get_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
):

    if api_key_query == API_KEY:
        return api_key_query
    elif api_key_header == API_KEY:
        return api_key_header
    elif api_key_cookie == API_KEY:
        return api_key_cookie
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
        )
        
@app.get("/")
async def homepage():
    return "Welcome to the security test!"

@app.get("/openapi.json", tags=["documentation"])
async def get_open_api_endpoint(api_key: APIKey = Depends(get_api_key)):
    response = JSONResponse(
        get_openapi(title="FastAPI security test", version=1, routes=app.routes)
    )
    return response

@app.get("/documentation", tags=["documentation"])
async def get_documentation(api_key: APIKey = Depends(get_api_key)):
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
    response.set_cookie(
        API_KEY_NAME,
        value=api_key,
        domain=COOKIE_DOMAIN,
        httponly=True,
        max_age=1800,
        expires=1800,
    )
    return response

@app.get("/logout")
async def route_logout_and_remove_cookie():
    response = RedirectResponse(url="/")
    response.delete_cookie(API_KEY_NAME, domain=COOKIE_DOMAIN)
    return response

@app.get("/secure_endpoint", tags=["test"])
async def get_open_api_endpoint(api_key: APIKey = Depends(get_api_key)):
    response = "How cool is this?"
    return response


class filter_sql(BaseModel):
    column: str
    value_filter: Tuple[str,...]= tuple()

class API_params(BaseModel):
    columns: List[str]
    filters: List[filter_sql]
    graph_columns: Optional[List[str]]
    metrics: List[str]
    granularity: Tuple[str,...]
    date_filter: Optional[Dict]
    ingestion_date: constr(regex=r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
    
def sort_model(filters: List[filter_sql] = Body(...), columns: List[str] = Body(...),graph_columns: Optional[List[str]] = Body(...),metrics: List[str] = Body(...),granularity: Tuple[str,...] = Body(...),date_filter: Optional[Dict]=Body(...),ingestion_date: constr(regex=r'[0-9]{4}-[0-9]{2}-[0-9]{2}')=Body(...),api_key: APIKey = Depends(get_api_key)):
    return API_params(filters=filters, columns=columns,graph_columns=graph_columns,metrics=metrics,granularity=granularity,date_filter=date_filter,ingestion_date=ingestion_date)

def sort_sql_filter(column: str, value_filter: List[str]):
    return filter_sql(column=column, value_filter=value_filter)

class DateParam(BaseModel):
    date_regex= r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    date: constr(regex=r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
    
    

def sort_date(date: str=Body(...),date_regex: Optional[str]=Body(...),api_key: APIKey = Depends(get_api_key)):
    return DateParam(date=date,date_regex= r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
