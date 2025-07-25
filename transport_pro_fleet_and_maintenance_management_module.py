import os
import pandas as pd
from datetime import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv()

#Function to handle NULL/BLANK in Integer data types.
def get_integer_value(value):
    if value == '':
        return None
    else:
        return int(value)

#Function to handle NULL/BLANK in Date data types.
def get_date_value(value):
    if value == '':
        return None
    else:
        return dt.strptime(value, '%Y-%m-%d')

try:
    #Establish connection.
    RAILWAY_DB_URL = os.getenv("RAILWAY_DB_URL")
    engine = create_engine(RAILWAY_DB_URL, connect_args={'options': '-c search_path=transport_pro'})

    #Query to create new records.
    new_record_type = input('Enter record type (Fleet/Maintenance): ')
    if new_record_type == 'Fleet':
        #Query to create a new fleet.
        new_vehicle_fleet_type = input('Fleet Type: ')
        new_vehicle_brand_name = input('Vehicle Brand Name: ')
        new_vehicle_fleet_category = input('Fleet Category: ')
        new_vehicle_model_year = get_integer_value(input('Model Year: '))
        new_vehicle_total_seats_including_driver = get_integer_value(input('Total Seats (including driver): '))
        new_vehicle_mulkiya_number = input('Mulkiya Number: ')
        new_vehicle_mulkiya_expiry_date = get_date_value(input('Mulkiya Expiry Date: '))
        new_vehicle_salik_tag_number = input('Salik Tag Number: ')
        new_vehicle_insurance_number = input('Insurance Number: ')
        new_vehicle_insurance_expiry_date = get_date_value(input('Insurance Expiry Date: '))
        new_vehicle_km_reading = get_integer_value(input('KM Reading: '))
        new_vehicle_status = input('Status: ')
        new_vehicle_acquisition_cost = get_integer_value(input('Acquisition Cost: '))
        new_vehicle_monthly_rental_cost = get_integer_value(input('Monthly Rental Cost: '))
        new_vehicle_useful_life = get_integer_value(input('Useful Life (in years): '))
        new_vehicle_rental_period_in_months = get_integer_value(input('Rental Period (in months): '))
        new_vehicle_period_of_use_in_months = get_integer_value(input('Period of Use (in months): '))

        insert_new_fleet = text("""
                                        insert into fleet_master(
                                            secondary_key_org,
                                            fleet_type,
                                            vehicle_brand_name,
                                            fleet_category,
                                            vehicle_model_year,
                                            vehicle_total_seats_including_driver,
                                            mulkiya_number,
                                            mulkiya_expiry_date,
                                            salik_tag_number,
                                            vehicle_insurance_number,
                                            vehicle_insurance_expiry_date,
                                            vehicle_km_reading,
                                            vehicle_status,
                                            vehicle_acquisition_cost,
                                            vehicle_monthly_rental_cost,
                                            vehicle_useful_life,
                                            vehicle_rental_period_in_months,
                                            vehicle_period_of_use_in_months
                                        )values(
                                            :secondary_key_org,
                                            :fleet_type,
                                            :vehicle_brand_name,
                                            :fleet_category,
                                            :vehicle_model_year,
                                            :vehicle_total_seats_including_driver,
                                            :mulkiya_number,
                                            :mulkiya_expiry_date,
                                            :salik_tag_number,
                                            :vehicle_insurance_number,
                                            :vehicle_insurance_expiry_date,
                                            :vehicle_km_reading,
                                            :vehicle_status,
                                            :vehicle_acquisition_cost,
                                            :vehicle_monthly_rental_cost,
                                            :vehicle_useful_life,
                                            :vehicle_rental_period_in_months,
                                            :vehicle_period_of_use_in_months
                                        )""")
        with engine.begin() as conn:
            conn.execute(insert_new_fleet, {
                                            'secondary_key_org': 1,
                                            'fleet_type': new_vehicle_fleet_type,
                                            'vehicle_brand_name': new_vehicle_brand_name,
                                            'fleet_category': new_vehicle_fleet_category,
                                            'vehicle_model_year': new_vehicle_model_year,
                                            'vehicle_total_seats_including_driver': new_vehicle_total_seats_including_driver,
                                            'mulkiya_number': new_vehicle_mulkiya_number,
                                            'mulkiya_expiry_date': new_vehicle_mulkiya_expiry_date,
                                            'salik_tag_number': new_vehicle_salik_tag_number,
                                            'vehicle_insurance_number': new_vehicle_insurance_number,
                                            'vehicle_insurance_expiry_date': new_vehicle_insurance_expiry_date,
                                            'vehicle_km_reading': new_vehicle_km_reading,
                                            'vehicle_status': new_vehicle_status,
                                            'vehicle_acquisition_cost': new_vehicle_acquisition_cost,
                                            'vehicle_monthly_rental_cost': new_vehicle_monthly_rental_cost,
                                            'vehicle_useful_life': new_vehicle_useful_life,
                                            'vehicle_rental_period_in_months': new_vehicle_rental_period_in_months,
                                            'vehicle_period_of_use_in_months': new_vehicle_period_of_use_in_months
            })
    elif new_record_type == 'Maintenance':
        #Query to create a new maintenance record.
        new_maintenance_vehicle = get_integer_value(input('Select Vehicle (from list of vehicles): '))
        new_maintenance_type = input('Select Maintenance Type: ')
        new_maintenance_date = get_date_value(input('Select Maintenance Date: '))
        new_maintenance_job_description = input('Maintenance Job Description: ')
        new_maintenance_workshop = input('Select Maintenance Workshop: ')
        new_maintenance_cost = get_integer_value(input('Maintenance Cost: '))
        new_pre_maintenance_url = input('Upload Pre-Maintenance Image: ')

        insert_new_maintenance = text("""
                                        insert into maintenance_master(
                                            secondary_key_fleet,
                                            maintenance_type,
                                            maintenance_date,
                                            maintenance_job_description,
                                            maintenance_workshop,
                                            maintenance_cost,
                                            pre_maintenance_image_url,
                                            post_maintenance_image_url
                                        )values(
                                            :secondary_key_fleet,
                                            :maintenance_type,
                                            :maintenance_date,
                                            :maintenance_job_description,
                                            :maintenance_workshop,
                                            :maintenance_cost,
                                            :pre_maintenance_image_url,
                                            :post_maintenance_image_url
                                        )
                                        """)

        with engine.begin() as con:
            con.execute(insert_new_maintenance, {
            'secondary_key_fleet': new_maintenance_vehicle,
            'maintenance_type': new_maintenance_type,
            'maintenance_date': new_maintenance_date,
            'maintenance_job_description': new_maintenance_job_description,
            'maintenance_workshop': new_maintenance_workshop,
            'maintenance_cost': new_maintenance_cost,
            'pre_maintenance_image_url': new_pre_maintenance_url
            })
    else:
        print('No valid option selected')


    #Query the view to use in the display results area.
    query_display = 'select * from fleet_and_maintenance_view;'
    df = pd.read_sql(query_display, engine)
    print(f'Fleet and Maintenance Data:\n{df}')
except Exception as e:
    print(f'Failed to connect or query:{e}')