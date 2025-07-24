import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv()
try:
    #Establish connection.
    RAILWAY_DB_URL = os.getenv("RAILWAY_DB_URL")
    engine = create_engine(RAILWAY_DB_URL, connect_args={'options': '-c search_path=transport_pro'})

    #Query to create new records.
    #Query to create a new department.
    new_department_id = input('Department ID: ')
    new_department_name = input('Department Name: ')
    insert_new_department = text("""
                                    insert into department_master(
                                        secondary_key_org,
                                        department_id,
                                        department_name
                                    )values(
                                        :secondary_key_org,
                                        :department_id,
                                        :department_name
                                    )""")
    with engine.begin() as conn:
        conn.execute(insert_new_department, {
                                        'secondary_key_org': 1,
                                        'department_id': new_department_id,
                                        'department_name': new_department_name
        })


    #Query the view to use in the display results area.
    query_display = 'select * from organization_management_view;'
    df = pd.read_sql(query_display, engine)
    print(f'Organization Data:\n{df}')
except Exception as e:
    print(f'Failed to connect or query:{e}')