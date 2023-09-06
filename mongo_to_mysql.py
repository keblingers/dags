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
from datetime import datetime, date
import pandas as pd
from json import dumps
import json
from bson import ObjectId

def load_mongo_data():
    # try:
    mongo_hook = MongoHook(mongo_conn_id = 'mongo_kebsas02')
    mongo_conn = mongo_hook.get_conn()
    mongo_db = mongo_conn.keblingers
    mongo_collection = mongo_db.stock_ticker
    mongo_query = pd.DataFrame(list(mongo_collection.find()))
    print(mongo_query)
    # except Exception as e:
    #     print("error connecting to mongodb -- {e}")

def mongo_data():
    try:
        #base_conn = Basehook.get_connection('mongo_kebsas02')
        mongo_hook = MongoHook(conn_id='mongo_kebsas02')
        collection = 'stock_ticker'
        db = 'keblingers'
        config_connection = mongo_hook.get_collection(mongo_db=db,mongo_collection=collection)
        result = pd.DataFrame(list(config_connection.find()))
        del result['_id']
        #json.JSONEncoder().encode(result['_id'])
        source_to_json = result.to_json(orient='records', date_format='iso', date_unit='us')
        #a = json.JSONEncoder().encode(source_to_json)
        #return result
        print(result)
        #print(a)
        return source_to_json

    except Exception as e:
        print(f"error connecting to mongodb -- {e}")

def load_to_mysql():
    data = pd.read_json(mongo_data())
    #print(data)
    table = 'mongo_mysql'
    data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    data["update_date"] = pd.to_datetime(data["update_date"], format="%Y-%m-%dT%H:%M:%S.%f")
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

with DAG('mongo_to_mysql',
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