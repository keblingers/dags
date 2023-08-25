import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import pandas as pd
from datetime import datetime, date

def get_data():
    source_hook = MySqlHook(mysql_conn_id='mysql_dev')
    source_conn = source_hook.get_conn()
    query = """select b.borrower_id,
            b.created_date,
            b.updated_date,
            b.partner_id,
            b.applicant_id,
            b.borrower_status,
            b.borrower_type,
            b.borrower_gender,
            b.borrower_email,
            b.borrower_name,
            b.borrower_phone,
            b.borrower_birth_place,
            b.borrower_birth_date,
            b.borrower_marital_status,
            b.mother_maiden_name,
            b.borrower_married_certificate,
            b.borrower_religion,
            b.borrower_last_education,
            b.borrower_job,
            b.borrower_kk_number,
            b.borrower_ktp_number,
            b.borrower_ktp_type,
            b.borrower_ktp_expired_date,
            b.borrower_ktp_address_id,
            b.borrower_domicile_address_id,
            b.borrower_domicile_ownership,
            b.borrower_bank_id,
            b.borrower_bank_name,
            b.borrower_bank_account_number,
            b.borrower_bank_owner_name,
            b.borrower_relationship,
            b.borrower_relation_name,
            b.borrower_relation_ktp_number,
            b.borrower_relation_ktp_type,
            b.borrower_relation_ktp_expired_date,
            b.borrower_relation_phone,
            b.borrower_relation_email,
            b.borrower_relation_address_id,
            b.borrower_npwp,
            b.borrower_company_name,
            b.borrower_company_description,
            b.borrower_industrial_type,
            b.borrower_product_category,
            b.borrower_company_establishment_date,
            b.borrower_company_total_employee,
            b.borrower_company_address_id,
            b.borrower_company_place_ownership,
            b.borrower_company_phone,
            b.monthly_profit_margin,
            bd.borrower_company_npwp,
            bd.borrower_company_deed_number,
            bd.borrower_company_establishment_sk_number,
            bd.borrower_company_change_deed_number,
            bd.borrower_company_legal_letter_owning,
            bd.borrower_company_nib_number,
            bd.borrower_company_siup_number,
            l.loan_id,
            l.created_date,
            l.updated_date,
            l.loan_number,
            l.loan_amount,
            l.loan_tenor,
            l.loan_status,
            l.loan_has_other_provider,
            l.product_type
            from borrower b 
            left join borrower_documentation bd on b.borrower_id = bd.borrower_id
            left join loan l on b.borrower_id = l.borrower_id
            where borrower_status = "ON REVIEW""""
    source_sql = pd.read_sql(query,con=source_conn)
    df_to_json = df.to_json(orient='records',date_format='iso',date_unit='us')

    return df_to_json

def convert_to_excel():
    data = get_data()
    now = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    writer = pd.ExcelWriter(f'view_loan_non_apply_{now}.xlsx',engine='openpyxl')
    data.to_excel(writer,sheet_name="main",index=False)

