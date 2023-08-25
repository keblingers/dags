import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.email_operator import EmailOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime, date
import pandas as pd
from json import dumps

def start_task():
    print("start task")

def failed_tasks():
    source_hook = MySqlHook(mysql_conn_id = 'mysql_failed')
    source_conn = source_hook.get_conn()
    source_sql = pd.read_sql("select * from metrics",con=source_conn)
    df = pd.DataFrame(source_sql)

    return df

default_args = {
    'owner' : 'airflow',
    'email' : ['fatah@sahabatbisnis.co.id'],
    'email_on_failure' : True
}

with DAG('email',
    default_args=default_args,
    schedule_interval= Variable.get("scheduled_1"),
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id = 'execute',
        python_callable = start_task
    )

    failed_task = PythonOperator(
        task_id = 'failed_query',
        python_callable=failed_tasks
    )

    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'fatah@sahabatbisnis.co.id',
        subject = 'test send email airflow',
        html_content="date : {{ ds }}"
    )

start_task >> failed_task >> send_email