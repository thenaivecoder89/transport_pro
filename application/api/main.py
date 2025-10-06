from fastapi import FastAPI
from application.database.Python_scripts.organization_db import get_organization_master_data as org_master

app = FastAPI()

@app.get('/')
def test_railway():
    output = {'message': 'Hello World!'}
    return output

@app.get('/get_organization_master_data')
def get_organization_master_data():
    output_organization_master, output_organization_master_json = org_master()
    return output_organization_master_json