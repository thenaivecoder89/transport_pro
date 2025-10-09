from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import json
from application.database.Python_scripts.organization_db import get_organization_master_data as org_master
import os
import logging
from starlette.middleware.base import BaseHTTPMiddleware

# Configure logging
logging.basicConfig(
    filename="api_access.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class IPLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Get the client IP, respecting proxies/load balancers if any
        x_forwarded_for = request.headers.get("x-forwarded-for")
        client_ip = (
            x_forwarded_for.split(",")[0].strip()
            if x_forwarded_for
            else request.client.host
        )

        # Optional: log path, method, and IP
        logging.info(f"{request.method} {request.url.path} accessed from IP: {client_ip}")

        # Proceed to actual route
        response = await call_next(request)
        return response

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
app.add_middleware(IPLoggerMiddleware) # IPLogger to log all IP addresses that access the application.

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