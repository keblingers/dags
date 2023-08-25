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
import yfinance as yf
import telegram_send
from datetime import datetime

def get_time():
    today = datetime.today()
    print(today)

default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('scheduled_variable',
    default_args=default_args,
    schedule_interval=Variable.get("scheduled_1"),
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    today = PythonOperator(
        task_id = 'get_time',
        python_callable = get_time
    )

today