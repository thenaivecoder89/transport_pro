import datetime as dt
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text

# Initialize environment
load_dotenv()
DB_URL = os.getenv('RAILWAY_DB_URL')
engine = create_engine(DB_URL, connect_args={'options':'-c search_path="transport_pro"'})

# Select and view data
def get_organization_master_data():
    try:
        organization_master_query = text('select * from organization_master')
        with engine.connect() as conn:
            df_organization_master = pd.read_sql(sql=organization_master_query, con=conn, index_col='org_id_pk')
            df_organization_master['created_at'] = pd.to_datetime(df_organization_master['created_at'], unit='ms')
        return df_organization_master, df_organization_master.to_json(orient='records')
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message