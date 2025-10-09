import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import bcrypt

# Initializing environment
load_dotenv()
DB_URL = os.getenv('RAILWAY_DB_URL')
engine = create_engine(DB_URL, connect_args={'options': '-c search_path="transport_pro"'})

# Select and view data
def get_users_and_allocated_roles():
    try:
        get_user_roles = text('select * from user_role_view')
        with engine.begin() as conn:
            df_user_roles = pd.read_sql(sql=get_user_roles, con=conn, index_col='user_id_pk')
            df_user_roles['created_at'] = df_user_roles['created_at'].astype(str)
            df_user_roles['user_uuid'] = df_user_roles['user_uuid'].astype(str)
            return df_user_roles, df_user_roles.to_json(orient='records')
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return None, error_message

# Create users
def create_new_user(payload: dict):
    try:
        get_org_id = text('select org_id_pk from organization_master')
        with engine.begin() as conn:
            df_org_id = pd.read_sql(sql=get_org_id, con=conn)
        org_id = payload.get('user_org_id_fk')
        org_id_exists = df_org_id['org_id_pk'].eq(org_id).any()
        if org_id_exists:
            plain_password = payload.get('password_hash')
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), salt).decode('utf-8')
            insert_into_user_master = text("""
            insert into user_master(
            user_role_id_fk,
            user_org_id_fk,
            username,
            password_hash,
            user_active_inactive_flag,
            created_by
            )values(
            :user_role_id_fk,
            :user_org_id_fk,
            :username,
            :password_hash,
            :user_active_inactive_flag,
            :created_by
            )
            """)
            params_insert = {
                'user_role_id_fk': payload.get('user_role_id_fk'),
                'user_org_id_fk': payload.get('user_org_id_fk'),
                'username': payload.get('username'),
                'password_hash': hashed_password,
                'user_active_inactive_flag': payload.get('user_active_inactive_flag'),
                'created_by': payload.get('created_by'),
            }
            with engine.begin() as conn:
                conn.execute(insert_into_user_master, params_insert)
                success_message = {'Successfully created user: ': {payload.get('username')}}
                return success_message
        else:
            error_message = {'Organization ID not found.'}
            return error_message
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Update users
def update_existing_user(payload: dict):
    try:
        get_org_id = text('select org_id_pk from organization_master')
        with engine.begin() as conn:
            df_org_id = pd.read_sql(sql=get_org_id, con=conn)
        org_id = payload.get('org_id')
        org_id_exists = df_org_id['org_id_pk'].eq(org_id).any()
        if org_id_exists:
            plain_password = payload.get('password_hash')
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), salt).decode('utf-8')
            update_user_master = text("""
            update user_master
            set
            user_role_id_fk = :user_role_id_fk,
            user_org_id_fk = :user_org_id_fk,
            username = :username,
            password_hash = :password_hash,
            user_active_inactive_flag = :user_active_inactive_flag,
            created_by = :created_by
            where user_uuid = :user_uuid
            and user_org_id_fk = :user_org_id_fk
            """)
            params_update = {
                'user_org_id_fk': payload.get('org_id'),
                'user_uuid': payload.get('user_uuid'),
                'user_role_id_fk': payload.get('user_role_id_fk'),
                'username': payload.get('username'),
                'password_hash': hashed_password,
                'user_active_inactive_flag': payload.get('user_active_inactive_flag'),
                'created_by': payload.get('created_by')
            }
            with engine.begin() as conn:
                conn.execute(update_user_master, params_update)
                success_message = {'Successfully updated user: ': {payload.get('username')}}
                return success_message
        else:
            error_message = {'Organization ID not found.'}
            return error_message
    except Exception as e:
        error_message = {'Error Encountered': {e}}
        return error_message

# Login and access
def login_and_access(payload: dict):
    try:
        username_password_extract_query = text("""
        select
        username,
        password_hash,
        full_system_access_flag,
        hr_system_access_flag,
        procurement_system_access_flag,
        contracts_system_access_flag,
        fleet_management_system_access_flag,
        maintenance_system_access_flag,
        inventory_system_access_flag,
        finance_system_access_flag
        from
        user_role_view
        where
        username = :username
        """)
        params_login = {
            'username': payload.get('username')
        }
        with engine.begin() as conn:
            df_get_users = conn.execute(username_password_extract_query, params_login).mappings().first()
        username = payload.get('username')
        password_str = payload.get('password_str')
        stored_password = df_get_users['password_hash']
        stored_password = stored_password.encode('utf-8')
        received_password = password_str.encode('utf-8')
        check_passwords = bcrypt.checkpw(received_password, stored_password)
        if check_passwords:
            full_system_access = df_get_users['full_system_access_flag']
            hr_system_access = df_get_users['hr_system_access_flag']
            procurement_system_access = df_get_users['procurement_system_access_flag']
            contracts_system_access = df_get_users['contracts_system_access_flag']
            fleet_management_system_access = df_get_users['fleet_management_system_access_flag']
            maintenance_system_access = df_get_users['maintenance_system_access_flag']
            inventory_system_access = df_get_users['inventory_system_access_flag']
            finance_system_access = df_get_users['finance_system_access_flag']
            if full_system_access == 'Y':
                access_message = {
                                    'Success! Password matched for user: ': username,
                                    'Full System Access': full_system_access
                                  }
            else:
                if hr_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'HR System Access': hr_system_access
                    }
                elif procurement_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Procurement System Access': procurement_system_access
                    }
                elif contracts_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Contracts System Access': contracts_system_access
                    }
                elif fleet_management_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Fleet Management System Access': fleet_management_system_access
                    }
                elif maintenance_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Maintenance System Access': maintenance_system_access
                    }
                elif inventory_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Inventory System Access': inventory_system_access
                    }
                elif finance_system_access == 'Y':
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Finance System Access': finance_system_access
                    }
                else:
                    access_message = {
                        'Success! Password matched for user: ': username,
                        'Error in System Access': 'No specific access found'
                    }
            return access_message
        else:
            return {'Failure! Password did not match for user: ': username}
    except Exception as e:
        error_message = {'Error encountered and unable to login. Please contact system administrator for assistance and provide the following error details: ': {e}}
        return error_message

def get_roles():
    get_system_roles = text('select role_name, role_description from role_master')
    with engine.begin() as conn:
        df_list_roles = pd.read_sql(sql=get_system_roles, con=conn)

    return df_list_roles.to_json(orient='records')

# Manual test of select and view functionality -- commented for prod deployment, applicable only for local testing.
# df, df_json = get_users_and_allocated_roles()
# print(f'Data in dataframe:\n{df}')
# print(f'Data in JSON format:\n{df_json}')

# Manual test of insert functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'user_role_id_fk': 1,
#     'user_org_id_fk': 1,
#     'username': 'sys_admin',
#     'password_hash': 'Admin_UE@1234',
#     'user_active_inactive_flag': 'A',
#     'created_by': 'SYSTEM'
# }
# print(create_new_user(payload))

# Manual test of update functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'org_id': 1,
#     'user_uuid': 'c2fd38fd-d6aa-44b0-9c89-8eb964b0efad',
#     'user_role_id_fk': 1,
#     'user_org_id_fk': 1,
#     'username': 'sys_admin',
#     'password_hash': 'UE_Admin@1234',
#     'user_active_inactive_flag': 'A',
#     'created_by': 'SYSTEM'
# }
# print(update_existing_user(payload))

# Manual test of login functionality -- commented for prod deployment, applicable only for local testing.
# payload = {
#     'username': 'sys_admin',
#     'password_str': 'UE_Admin@1234'
# }
# payload = {
#     'username': 'sys_owner',
#     'password_str': 'System_transportpro@1234'
# }
# print(login_and_access(payload))

# Manual test of generating roles list functionality -- commented for prod deployment, applicable only for local testing.
# print(get_roles())