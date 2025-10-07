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
        with engine.begin() as conn:
            df_organization_master = pd.read_sql(sql=organization_master_query, con=conn, index_col='org_id_pk')
            df_organization_master['created_at'] = df_organization_master['created_at'].astype(str)
        return df_organization_master, df_organization_master.to_json(orient='records')
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Create new organization
def create_organization(payload: dict):
    try:
        insert_into_organization_master = text("""
        insert into organization_master(
        org_name,
        org_address,
        org_phone_primary,
        org_phone_secondary,
        org_email_primary,
        org_email_secondary,
        company_registration_number,
        created_by
        )values(
        :org_name,
        :org_address,
        :org_phone_primary,
        :org_phone_secondary,
        :org_email_primary,
        :org_email_secondary,
        :company_registration_number,
        :created_by
        )
        """)
        with engine.begin() as conn:
            conn.execute(insert_into_organization_master, payload)
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Update existing organization
def update_organization(payload: dict):
    try:
        get_org_id = text('select org_id_pk from organization_master')
        with engine.begin() as conn:
            df_org_id = pd.read_sql(sql=get_org_id, con=conn)
        org_id = payload.get('org_id')
        org_id_exists = df_org_id['org_id_pk'].eq(org_id).any()
        if org_id_exists:
            update_organization_master = text("""
            update organization_master
            set 
            org_name = :org_name,
            org_address = :org_address,
            org_phone_primary = :org_phone_primary,
            org_phone_secondary = :org_phone_secondary,
            org_email_primary = :org_email_primary,
            org_email_secondary = :org_email_secondary,
            company_registration_number = :company_registration_number,
            created_by = :created_by
            where org_id_pk = :org_id
            """)
            params_update = {
                'org_id': payload.get('org_id'),
                'org_name': payload.get('org_name'),
                'org_address': payload.get('org_address'),
                'org_phone_primary': payload.get('org_phone_primary'),
                'org_phone_secondary': payload.get('org_phone_secondary'),
                'org_email_primary': payload.get('org_email_primary'),
                'org_email_secondary': payload.get('org_email_secondary'),
                'company_registration_number': payload.get('company_registration_number'),
                'created_by': payload.get('created_by')
            }
            with engine.begin() as conn:
                conn.execute(update_organization_master, params_update)
        else:
            return {'Organization ID not found.'}
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Delete organization
def delete_organization(payload: dict):
    try:
        get_org_id = text('select org_id_pk from organization_master')
        with engine.begin() as conn:
            df_org_id = pd.read_sql(sql=get_org_id, con=conn)
        org_id = payload.get('org_id')
        org_id_exists = df_org_id['org_id_pk'].eq(org_id).any()
        if org_id_exists:
            delete_organization_master = text("""
            delete from organization_master where org_id_pk = :org_id
            """)
            params_delete = {
                'org_id': org_id
            }
            with engine.begin() as conn:
                conn.execute(delete_organization_master, params_delete)
        else:
            return {'Organization ID not found'}
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Manual test of select and view functionality -- commented for prod deployment, applicable only for local testing.
# df, df_json = get_organization_master_data()
# print(f'Data in dataframe:\n{df}')
# print(f'Data in JSON format:\n{df_json}')

# Manual test of update functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'org_id': 1,
#     'org_name': 'Urban Express',
#     'org_address': 'Office no 105, First Floor, B Block, Bel Rasheed Twin Towers, Al Qusais, Dubai, UAE',
#     'org_phone_primary': '+971 52 1124 424',
#     'org_phone_secondary': '+971 52 1134 434',
#     'org_email_primary': 'info@urbanexpress.ae',
#     'org_email_secondary': '',
#     'company_registration_number': '',
#     'created_by': 'SYSTEM_UPDATE'
# }
# update_organization(payload)

# Manual test of insert functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'org_name': 'Urban Exp',
#     'org_address': 'Office no 105, First Floor, B Block, Bel Rasheed Twin Towers, Al Qusais, Dubai, UAE',
#     'org_phone_primary': '+971 52 1124 424',
#     'org_phone_secondary': '+971 52 1134 434',
#     'org_email_primary': 'info@urbanexpress.ae',
#     'org_email_secondary': '',
#     'company_registration_number': '',
#     'created_by': 'SYSTEM_UPDATE'
# }
# create_organization(payload)

# Manual test of delete functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'org_id': 3
# }
# delete_organization(payload)