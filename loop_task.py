import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.http_hook import HttpHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import pandas as pd
import requests
from pprint import pprint
from json import dumps

def pull_mysql1(ti):
    state = ti.xcom_pull(task_ids = ['mysql_connector_1'])
    state_mysql2 = ti.xcom_pull(task_ids = ['mysql_connector_2'])
    print(f'ini mysql1 :{state}')
    print(f'ini mysql2 : {state_mysql2}')

    # conn = []
    # conn.append((state, state_mysql2))
    # print(conn)
    
    if state == 'RUNNING':
        print('if mysql1')
        return 'mysql1_running'
    else:
        return 'mysql1_not_running'

    if state_mysql2 == 'RUNNING':
        print('if mysql2')
        return 'mysql2_running'
    else:
        return 'mysql2_not_running'


#@task
# def get_state():
#     list_connector = Variable.get("connector")
#     connector_string = "".join(list_connector)
#     connector = str(connector_string).split(",")
#     host = Variable.get('host')
#     res = requests.get('kebsas02:8083/connectors/mysql_connector/status')
#     conn_state = res.json()['connector']['state']
#     task_state = res.json()['tasks'][0]['state']
#     if conn_state == 'RUNNING' and task_state == 'RUNNING':
#             return connector_running
#     else:
#             return connector_not_running
    #connector_host
    # for x in connector:
    #     #res = requests.get(f'{host}:8083/connectors/{x}/status')
    #     res = HttpHook(http_conn_id = "kafka-connector", method ='GET')
    #     respon = res.check_response()
    #     print(respon)
        # conn_state = res.json()['connector']['state']
        # task_state = res.json()['tasks'][0]['state']
        # conn = dumps(conn_state, default=json_serial)
        # task = dumps(task_state, default=json_serial)
        #task_state = 'FAILED'
        #json_format = (json.load(res))
        #state = json_format['connector']['id']
        #pprint(state)
        #pprint(json_format)
        #pprint(res.json())
        # pprint(f'connector {conn_state}')
        # pprint(f'task : {task_state}')
        # print(f'connector {conn}')
        # print(f'task : {task}')
        # if conn_state == 'RUNNING' and task_state == 'RUNNING':
        #     return connector_running
        # else:
        #     return connector_not_running




default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('loop_task',
    default_args=default_args,
    schedule_interval='0 3-9 * * 1-5',
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    mysql_connector_1 = SimpleHttpOperator(
        task_id = 'mysql_connector_1',
        http_conn_id = 'kafka-connect',
        endpoint = 'mysql-connector/status',
        method = 'GET',
        response_filter = lambda response: response.json()['tasks'][0]['state']
    )

    mysql_connector_2 = SimpleHttpOperator(
        task_id = 'mysql_connector_2',
        http_conn_id = 'kafka-connect',
        endpoint = 'connector-baru/status',
        method = 'GET',
        response_filter = lambda response: response.json()['tasks'][0]['state']
    )

    pull_mysql1 = BranchPythonOperator(
        task_id = 'pull_mysql1',
        python_callable = pull_mysql1,
        #provide_context = True
    )

    mysql1_running = BashOperator(
        task_id = 'mysql1_running',
        bash_command = 'echo connector is running'
    )

    mysql2_running = BashOperator(
        task_id = 'mysql2_running',
        bash_command = 'echo connector is running'
    )

    mysql1_not_running = BashOperator(
        task_id = 'mysql1_not_running',
        bash_command = 'echo connector mysql1 is not running'
    )

    mysql2_not_running = BashOperator(
        task_id = 'mysql2_not_running',
        bash_command = 'echo connector mysql2 is not running'
    )

#     connector_not_running = EmailOperator(
#         task_id = 'connector_not_running',
#         to = Variable.get('email'),
#         subject = 'test loop connector',
#         html_content='please check'
#     )

#     connector_state = PythonOperator(
#         task_id = 'connector_state',
#         python_callable = get_state
#     )

# connector_state >> [connector_running,connector_not_running]

[mysql_connector_1,mysql_connector_2] >> pull_mysql1 >> [mysql1_running,mysql2_running,mysql1_not_running,mysql2_not_running]