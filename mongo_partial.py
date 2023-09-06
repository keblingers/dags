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
    conn = create_engine('mysql://{conn_hook.login}:{conn_hook.password}@{conn_hook.host}/{conn_hook.schema}')
    data = pd.read_sql(f"select sync_status_time from sync_status where sync_status_table = '{table_name}'",con=conn)
    sync_time = data['sync_status_time'].iloc[0]
    a = sync_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    b = str(a)
    return b

def mongo_data():
    try:
        table = 'mongo_collection'
        sync_time = get_sync_time(f'{table}')
        a = datetime.strptime(sync_time,"%Y-%m-%dT%H:%M:%S.%fZ")
        b = a.astimezone(pytz.UTC)

        #print(b)
        
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
    
def temp_mongo_data():
    temp_data = pd.read_json(mongo_data())
    if temp_data.shape[0] != 0:
        temp_data["created_date"] = pd.to_datetime(temp_data["created_date"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        temp_data["update_date"] = pd.to_datetime(temp_data["create_date"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        table = 'mongo_collection_temp'

        connection = BaseHook.get_connection('mysql_ihsg')
        engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
        temp_data.to_sql(f'{table}', con=engine, if_exists='append', index=False)
    else:
        print('no dataaa')

def delete_existing_data():
    table = 'mongo_collection'
    temp_table = 'mongo_collection_temp'
    key = 'id'

    connection = BaseHook.get_connection('mysql_ihsg')
    engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
    clear_data = engine.execute(f"delete from {table} where {key} in (select {key} from {temp_table})")
    clear_data.close()
        

def load_to_mysql():
    data = pd.read_json(mongo_data())
    table = 'mongo_collection'

    if data.shape[0] != 0:
        try:
            data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
            data["update_date"] = pd.to_datetime(data["update_date"], format="%Y-%m-%dT%H:%M:%S.%f")
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
    else:
        print('no data')
        try:
            connection = BaseHook.get_connection('mysql_ihsg')
            engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
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

with DAG('mongo_partial',
    default_args = default_args,
    schedule_interval = '@once',
    start_date = datetime(2022,4,20),
    catchup = False
) as dag:

    extract_data_partial = PythonOperator(
    task_id = 'mongo_data',
    python_callable = mongo_data
    )

    load_data_partial = PythonOperator(
    task_id = 'load_to_mysql',
    python_callable = load_to_mysql
    )


extract_data_partial >> load_data_partial