from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
from application.database.Python_scripts.organization_db import get_organization_master_data as org_master
import os

app = FastAPI(title='Transport Pro APIs')

# CORS (Cross-Origin Resource Sharing) configuration - this allows external applications and websites to access this API
raw = os.getenv('ALLOWED_ORIGINS', '*') # This is the allowed list of websites that can access this API. '*' means anyone can access.
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=False
)

@app.get('/', status_code=200)
def test_railway():
    output = {'message': 'Hello World!'}
    return output

@app.get('/get_organization_master_data', status_code=200)
def get_organization_master_data():
    try:
        output_organization_master, output_organization_master_json = org_master()
        return json.loads(output_organization_master_json)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))