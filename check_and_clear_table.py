import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import pandas as pd

def check_table_size():
    source_hook = MySqlHook(mysql_conn_id='mysql_ihsg')
    source_conn = source_hook.get_conn()
    query = """SELECT
            TABLE_NAME AS `Table`,
            ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS `Size_(MB)`
            FROM
            information_schema.TABLES
            WHERE
            TABLE_SCHEMA = "temporary_test"
            and table_name = 'business_category'
            ORDER BY
            (DATA_LENGTH + INDEX_LENGTH)
            DESC;"""
    source_sql = pd.read_sql(query,con=source_conn)
    table_size = source_sql["Size_(MB)"].iloc[0]

    return table_size

def calculate_percentage():
    data = check_table_size()
    persen = (data / 10) * 100 
    print(persen)
    if 50 < persen <= 60:
        return "alert"
    elif persen > 60:
        return "truncate"
    else:
        return "table_size_normal"


default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('check_and_clear',
    default_args=default_args,
    schedule_interval= "@once",
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    get_table_size = PythonOperator(
        task_id = "get_table_size",
        python_callable= check_table_size,
    )

    get_precentage = BranchPythonOperator(
        task_id = "get_precentage",
        python_callable = calculate_percentage,
        provide_context= True
    )

    size_normal = BashOperator(
        task_id = "table_size_normal",
        bash_command = "echo table size is under 50%"
    )

    send_alert = EmailOperator(
        task_id = 'alert',
        to = Variable.get("to_alert_notification"),
        subject = "table size more than 50%",
        html_content = "please check table, because table size is more than 50%"
    )

    truncate_table = BashOperator(
        task_id = "truncate",
        bash_command = "echo table is more than 59%"
    )

    

get_table_size >> get_precentage >> [size_normal,send_alert,truncate_table]