from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime, date
import pandas as pd
from json import dumps

default_args = {
    'owner': 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : True
}

with DAG('dynamic_table',
    default_args = default_args,
    schedule_interval = '1 * * * *',
    start_date = datetime(2022,4,20),
    catchup = False
) as dag:

    @task
    def get_data(x: str):
        source_hook = MySqlHook(mysql_conn_id = 'mysql_ihsg')
        source_conn = source_hook.get_conn()
        source_sql = pd.read_sql(f"select * from {x}", con = source_conn)
        df = pd.DataFrame(source_sql)
        source_to_json = df.to_json(orient='records', date_format='iso', date_unit = 'us')

        return source_to_json

    @task
    def load_data(y: str):
        data = pd.read_json(get_data(y))
        connection = BaseHook.get_connection('kebsas02-postgres')
        engine = create_engine(f"postgres://{connection.login}:{connection.password}@{connection.host}/{connection.schema}")
        data.to_sql(f'{y}',con = engine, if_exists='append',index = False)

    migrate_data = load_data.expand(y=['stock_buy'])