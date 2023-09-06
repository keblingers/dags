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
import pytz

now = datetime.today().astimezone(pytz.UTC)

def mongo_data():
    try:
        mongo_hook = MongoHook(conn_id = 'mongo_kebsas02')
        mongo_conn = mongo_hook.get_conn()
        mongo_db = mongo_conn.keblingers
        mongo_collection = mongo_db.stock_ticker
        result = pd.DataFrame(list(mongo_collection.find()))
        result.rename(columns = {'_id': 'id'}, inplace=True)
        print(result)
        source_to_json = result.to_json(orient='records', date_format='iso', date_unit='us',default_handler=str)

        return source_to_json

    except Exception as e:
        print(f"error connecting to mongodb -- {e}")

def load_to_mysql():
    data = pd.read_json(mongo_data())
    data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    data["update_date"] = pd.to_datetime(data["update_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    table = 'mongo_collection'
    sync_status = 'SUCCESS'
    try:
        connection = BaseHook.get_connection('mysql_ihsg')
        engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
        data.to_sql(f'{table}', con=engine, if_exists='append', index=False)

        check_sync = pd.read_sql(f"select sync_status_table from sync_status where sync_status_table = '{table}'",con=engine)
    
        if check_sync.shape[0] == 0:
            insert_sync = engine.execute(f"insert into sync_status(sync_status_table, sync_status_time, sync_status) values('{table}','{now}','{sync_status}')")
            insert_sync.close()
        else:
            with engine.connect() as conn:
                update_sync = engine.execute(f"update sync_status set sync_status_time = '{now}' where sync_status_table = '{table}'")
                conn.close()

    except Exception as e:
        print(f"error connecting to mysql -- {e}")

default_args = {
    'owner': 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : True
}

with DAG('mongo_full',
    default_args = default_args,
    schedule_interval = '@once',
    start_date = datetime(2022,4,20),
    catchup = False
) as dag:

    extract_data_full = PythonOperator(
    task_id = 'mongo_data',
    python_callable = mongo_data
    )

    load_data_full = PythonOperator(
    task_id = 'load_to_mysql',
    python_callable = load_to_mysql
    )


extract_data_full >> load_data_full
