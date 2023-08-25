from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import pandas as pd

now = datetime.today()

def extract_activity_log():
    try:
        mongo_hook = MongoHook(conn_id = 'testing_mdb')
        mongo_conn = mongo_hook.get_conn()
        mongo_db = mongo_conn.mdb_log
        mongo_collection = mongo_db.activity_log
        result = pd.DataFrame(list(mongo_collection.find()))
        #result.rename(columns = {'_id':'activity_log_id'},inplace=True)
        #result.drop(columns = ['_class'],inplace=True)
        source_to_json = result.to_json(orient='records',date_format='iso',date_unit='us',default_handler=str)

        return source_to_json
    except Exception as e:
        print(f"error connecting to mongodb -- {e}")
    
def load_activity_log():
    data = pd.read_json(extract_activity_log())
    data.rename(columns = {'_id':'activity_log_id'},inplace=True)
    data.drop(columns = ['_class'],inplace=True)
    data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    data["updated_date"] = pd.to_datetime(data["updated_date"], format="%Y-%m-%dT%H:%M:%S.%f")
    table = 'activity_log'
    sync_status = 'success'

    try:
        connection = BaseHook.get_connection('mysql_rog')
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

with DAG('test_log',
    default_args= default_args,
    schedule_interval = '@once',
    start_date=datetime(2023,8,15),
    catchup=False
) as dag:

    activity_log_data = PythonOperator(
        task_id = 'extract_activity_log',
        python_callable = extract_activity_log
    )

    put_activity_log = PythonOperator(
        task_id = 'load_activity_log',
        python_callable = load_activity_log
    )

    truncate_table = MySqlOperator(
        task_id = 'clear_data',
        mysql_conn_id = 'mysql_rog',
        sql = 'truncate_log_table.sql'
    )

activity_log_data >> truncate_table >> put_activity_log