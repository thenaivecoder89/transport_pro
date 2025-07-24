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
    new_record_type = input('Enter record type (Department/Employee): ')
    if new_record_type == 'Department':
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
    elif new_record_type == 'Employee':
        #Query to create a new employee - CHANGE TYPES HERE
        new_empl_id = input('ID:')
        new_empl_name = input('Name:')
        new_empl_designation = input('Designation:')
        new_empl_joining_date = input('Joining Date:')
        new_empl_department = int(input('Department Name:'))
        new_empl_nationality = input('Nationality:')
        new_empl_passport_number = input('Passport Number:')
        new_empl_passport_expiry_date = input('Passport Expiry Date:')
        new_empl_issuing_country = input('Passport Issuing Country:')
        new_empl_passport_with = input('Passport With:')
        new_empl_visa_number = input('UAE Visa Number:')
        new_empl_visa_expiry_date = input('UAE Visa Expiry Date:')
        new_empl_eid_number = input('Emirates ID Number:')
        new_empl_eid_expiry_date = input('Emirates ID Expiry Date:')
        new_empl_phone_company = input('Company Phone Number:')
        new_empl_phone_personal = input('Personal Phone Number:')
        new_empl_phone_home = input('Home Phone Number:')
        new_empl_email_company = input('Company Email ID:')
        new_empl_email_personal = input('Personal Email ID:')
        new_empl_base_address = input('Address in UAE:')
        new_empl_home_address = input('Home Address:')
        new_empl_dl_number = input('Drivers License Number:')
        new_empl_dl_type = input('Drivers License Type:')
        new_empl_permit_number = input('Permit Number:')
        new_empl_permit_expiry_date = input('Permit Expiry Date:')
        new_empl_permit_type = input('Permit Type:')
        new_empl_insurance_number = input('Insurance Number:')
        new_empl_insurance_expiry_date = input('Insurance Expiry Date:')
        new_empl_bank_name = input('Bank Name:')
        new_empl_bank_address = input('Bank Address:')
        new_empl_bank_account_number = input('Bank Account Number:')
        new_empl_monthly_basic_salary = input('Monthly Basic Salary:')
        new_empl_monthly_allowance = input('Monthly Allowance:')
        new_empl_monthly_accomodation = input('Monthly Accomodation:')
        insert_new_employee = text("""
                                        insert into employee_master(
                                            secondary_key_dept,
                                            employee_id,
                                            employee_name,
                                            employee_designation,
                                            joining_date,
                                            nationality,
                                            passport_number,
                                            passport_expiry_date,
                                            passport_issuing_country,
                                            passport_with,
                                            visa_number,
                                            visa_expiry_date,
                                            eid_number,
                                            eid_expiry_date,
                                            phone_number_company,
                                            phone_number_personal,
                                            phone_number_home,
                                            email_id_company,
                                            email_id_personal,
                                            address_in_base_location,
                                            address_in_home_location,
                                            dl_number,
                                            dl_expiry_date,
                                            dl_type,
                                            permit_number,
                                            permit_expiry_date,
                                            permit_type,
                                            insurance_number,
                                            insurance_expiry_date,
                                            bank_name,
                                            bank_address,
                                            bank_account_number,
                                            monthly_basic_salary,
                                            monthly_allowance,
                                            monthly_accomodation
                                        )values(
                                            :secondary_key_dept,
                                            :employee_id,
                                            :employee_name,
                                            :employee_designation,
                                            :joining_date,
                                            :nationality,
                                            :passport_number,
                                            :passport_expiry_date,
                                            :passport_issuing_country,
                                            :passport_with,
                                            :visa_number,
                                            :visa_expiry_date,
                                            :eid_number,
                                            :eid_expiry_date,
                                            :phone_number_company,
                                            :phone_number_personal,
                                            :phone_number_home,
                                            :email_id_company,
                                            :email_id_personal,
                                            :address_in_base_location,
                                            :address_in_home_location,
                                            :dl_number,
                                            :dl_expiry_date,
                                            :dl_type,
                                            :permit_number,
                                            :permit_expiry_date,
                                            :permit_type,
                                            :insurance_number,
                                            :insurance_expiry_date,
                                            :bank_name,
                                            :bank_address,
                                            :bank_account_number,
                                            :monthly_basic_salary,
                                            :monthly_allowance,
                                            :monthly_accomodation                                        
                                        )
                                        """)
        with engine.begin() as con:
            con.execute(insert_new_employee, {
            ':secondary_key_dept': new_empl_department,
            ':employee_id': new_empl_id,
            ':employee_name': new_empl_name,
            ':employee_designation': new_empl_designation,
            ':joining_date': new_empl_joining_date,
            ':nationality': new_empl_nationality,
            ':passport_number': new_empl_passport_number,
            ':passport_expiry_date': new_empl_passport_expiry_date,
            ':passport_issuing_country': new_empl_issuing_country,
            ':passport_with': new_empl_passport_with,
            ':visa_number': new_empl_visa_number,
            ':visa_expiry_date': new_empl_visa_expiry_date,
            ':eid_number': new_empl_eid_number,
            ':eid_expiry_date': new_empl_eid_expiry_date,
            ':phone_number_company': new_empl_phone_company,
            ':phone_number_personal': new_empl_phone_personal,
            ':phone_number_home': new_empl_phone_home,
            ':email_id_company': new_empl_email_company,
            ':email_id_personal': new_empl_email_personal,
            ':address_in_base_location': new_empl_base_address,
            ':address_in_home_location': new_empl_home_address,
            ':dl_number': new_empl_dl_number,
            ':dl_expiry_date': new_empl_permit_expiry_date,
            ':dl_type': new_empl_dl_type,
            ':permit_number': new_empl_permit_number,
            ':permit_expiry_date': new_empl_permit_expiry_date,
            ':permit_type': new_empl_permit_type,
            ':insurance_number': new_empl_insurance_number,
            ':insurance_expiry_date': new_empl_insurance_expiry_date,
            ':bank_name': new_empl_bank_name,
            ':bank_address': new_empl_bank_address,
            ':bank_account_number': new_empl_bank_account_number,
            ':monthly_basic_salary': new_empl_monthly_basic_salary,
            ':monthly_allowance': new_empl_monthly_allowance,
            ':monthly_accomodation': new_empl_monthly_accomodation
            })
    else:
        print('No valid option selected')

    #Query the view to use in the display results area.
    query_display = 'select * from organization_management_view;'
    df = pd.read_sql(query_display, engine)
    print(f'Organization Data:\n{df}')
except Exception as e:
    print(f'Failed to connect or query:{e}')