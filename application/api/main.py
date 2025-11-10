from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
from application.database.Python_scripts.organization_db import get_organization_master_data as org_master
from application.database.Python_scripts.rbac_db import login_and_access as log_acc
from application.database.Python_scripts.rbac_db import get_users_and_allocated_roles as usr_roles
from application.api.api_logger import DBAccessLogMiddleware
import os

# Setup app
app = FastAPI(title='Transport Pro APIs')

# Middleware configuration
raw = os.getenv('ALLOWED_ORIGINS', '*') # This is the allowed list of websites that can access this API. '*' means anyone can access.
app.add_middleware(
    CORSMiddleware, # CORS (Cross-Origin Resource Sharing) configuration - this allows external applications and websites to access this API
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=False
)
app.add_middleware(DBAccessLogMiddleware) # IPLogger to log all IP addresses that access the application.

# API test
@app.get('/', status_code=200)
def test_railway():
    output = {'message': 'Hello World!'}
    return output

# API to get organization master data
@app.get('/get_organization_master_data', status_code=200)
def get_organization_master_data():
    try:
        output_organization_master, output_organization_master_json = org_master()
        return json.loads(output_organization_master_json)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API to manage RBAC
@app.post('/login_access', status_code=200)
def login_access(payload: dict):
    try:
        output = log_acc(payload)
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API to get user details
@app.get('/get_all_user_details', status_code=200)
def get_all_user_details():
    try:
        all_user_detail_json = usr_roles()
        return json.loads(all_user_detail_json)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))