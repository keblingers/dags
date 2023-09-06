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

now = datetime.today()

def get_sync_time(table_name):
        mysql_hook = BaseHook.get_connection('mysql_rog')
        mysql_conn = create_engine(f'mysql://{mysql_hook.login}:{mysql_hook.password}@{mysql_hook.host}/{mysql_hook.schema}')
        time_sql = pd.read_sql(f"select sync_status_time from sync_status where sync_status_table = '{table_name}'",con=mysql_conn)
        time_data = time_sql['sync_status_time'].iloc[0]
        time_format = time_data.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        sync_time = str(time_format)
        #print(sync_time)

        return sync_time
    
def extract_activity_log():
    try:
        table = 'activity_log'
        sync_time = get_sync_time(table)
        time_format = datetime.strptime(sync_time,"%Y-%m-%dT%H:%M:%S.%fZ")
        mongo_hook = MongoHook(conn_id = 'testing_mdb')
        mongo_conn = mongo_hook.get_conn()
        mongo_db = mongo_conn.mdb_log
        mongo_collection = mongo_db.activity_log
        result = pd.DataFrame(list(mongo_collection.find({"updated_date": {"$gte": time_format, "$lte": now}})))
        result.rename(columns = {'_id':'activity_log_id'},inplace=True)
        #result.drop(columns = ['_class'],inplace=True)
        source_to_json = result.to_json(orient='records',date_format='iso',date_unit='us',default_handler=str)
        
        return source_to_json
    except Exception as e:
        print(f"error connecting to mongodb -- {e}")
    
def temp_activity_log():
    temp_data = pd.read_json(extract_activity_log())
    if temp_data.shape[0] != 0:
        temp_data["created_date"] = pd.to_datetime(temp_data["created_date"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        temp_data["update_date"] = pd.to_datetime(temp_data["create_date"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        table = 'temp_activity_log'

        connection = BaseHook.get_connection('mysql_rog')
        engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
        temp_data.to_sql(f'{table}', con=engine, if_exists='append', index=False)
    else:
        print('no dataaa')

def delete_existing_activity_log():
    table = 'activity_log'
    temp_table = 'temp_activity_log'
    key = 'activity_log_id'

    connection = BaseHook.get_connection('mysql_rog')
    engine = create_engine(f'mysql://{connection.login}:{connection.password}@{connection.host}/{connection.schema}')
    clear_data = engine.execute(f"delete {table} from {table} right join {temp_table} on {table}.{key} = {temp_table}.{key}")
    clear_data.close()

def load_activity_log():
    data = pd.read_json(extract_activity_log())
    table = 'activity_log'

    if data.shape[0] != 0:
        try:
            data["created_date"] = pd.to_datetime(data["created_date"], format="%Y-%m-%dT%H:%M:%S.%f")
            data["update_date"] = pd.to_datetime(data["update_date"], format="%Y-%m-%dT%H:%M:%S.%f")
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
    else:
        print('no data')
        try:
            connection = BaseHook.get_connection('mysql_rog')
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

with DAG('internal_log_partial',
    default_args= default_args,
    schedule_interval = '@once',
    start_date=datetime(2023,8,15),
    catchup=False
) as dag:

    activity_log_data = PythonOperator(
        task_id = 'extract_acitivity_log',
        python_callable = extract_activity_log
    )

    temp_activity_log_data = PythonOperator(
        task_id = 'temp_activity_log',
        python_callable = temp_activity_log
    )

    clear_existing_activity_log = PythonOperator(
        task_id = 'delete_existing_activity_log',
        python_callable = delete_existing_activity_log
    )

    put_activity_log = PythonOperator(
        task_id = 'load_activity_log',
        python_callable = load_activity_log
    )

    create_temp_table = MySqlOperator(
        task_id = 'create_temp_table',
        mysql_conn_id = 'mysql_rog',
        sql = 'create_temp_internal_log.sql',
        trigger_rule = 'none_failed'
    )

    drop_temp_table = MySqlOperator(
        task_id = 'drop_temp_table',
        mysql_conn_id = 'mysql_rog',
        sql = 'drop_temp_internal_log.sql',
        trigger_rule = 'none_failed'
    )

    drop_temp = MySqlOperator(
        task_id = 'drop_temp',
        mysql_conn_id = 'mysql_rog',
        sql = 'drop_temp_internal_log.sql',
        trigger_rule = 'none_failed'
    )

drop_temp >> activity_log_data >> create_temp_table >> temp_activity_log_data >> clear_existing_activity_log >> put_activity_log >> drop_temp_table