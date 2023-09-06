from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import pandas as pd
from json import dumps
import pytz

now = datetime.today().astimezone(pytz.UTC)

def get_sync_time(table_name):
    conn_hook = BaseHook.get_connection('mysql_ihsg')
    conn = create_engine(f'mysql://{conn_hook.login}:{conn_hook.password}@{conn_hook.host}/{conn_hook.schema}')
    data = pd.read_sql(f"select sync_status_time from sync_status where sync_status_table = '{table_name}'",con=conn)
    sync_time = data['sync_status_time'].iloc[0]
    a = sync_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    b = str(a)
    return b

def mongo_data():
    try:
        sync_time = get_sync_time('mongo_to_mysql')
        a = datetime.strptime(sync_time,"%Y-%m-%dT%H:%M:%S.%fZ")
        b = a.astimezone(pytz.UTC)
        
        mongo_hook = MongoHook(conn_id = 'mongo_kebsas02')
        mongo_conn = mongo_hook.get_conn()
        mongo_db = mongo_conn.keblingers
        mongo_collection = mongo_db.stock_ticker
        result = pd.DataFrame(list(mongo_collection.find({"created_date": {"$gte": b, "$lte": now}})))
        result.rename(columns = {'_id': 'id'}, inplace=True)
        print(result)
        source_to_json = result.to_json(orient='records', date_format='iso', date_unit='us',default_handler=str)

        return source_to_json

    except Exception as e:
        print(f"error connecting to mongodb -- {e}")

def load_to_mysql():
    data = pd.read_json(mongo_data())
    #print(data)
    data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    data["update_date"] = pd.to_datetime(data["update_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    #print(data)
    table = 'mongo_collection'
    try:
        connection = BaseHook.get_connection('mysql_ihsg')
        engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
        data.to_sql(f'{table}', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f"error connecting to mysql -- {e}")

default_args = {
    'owner': 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : True
}

with DAG('mongo_serialized',
    default_args = default_args,
    schedule_interval = '1 * * * *',
    start_date = datetime(2022,4,20),
    catchup = False
) as dag:

    extract_data = PythonOperator(
    task_id = 'mongo_data',
    python_callable = mongo_data
    )

    load_data = PythonOperator(
    task_id = 'load_to_mysql',
    python_callable = load_to_mysql
    )


extract_data >> load_data
