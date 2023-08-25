from airflow import DAG
from airflow.models import Variable
import pandas as pd
from datetime import datetime
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import glob
import os

now = datetime.now()
hari_ini = now.strftime('%d-%m-%Y')
report_dir = Variable.get("csv_dir")
#file = f'{report_dir}/test_{hari_ini}.csv'
file = f'{report_dir}/{hari_ini}.csv'

def housekeeping_file():
    files = glob.glob(f"{report_dir}/*.csv")
    for file in files:
        try:
            os.remove(file)
        except OSError as e:
            print("Error: %s : %s " % (f, e.strerror))

def get_data():
    engine = MySqlHook(mysql_conn_id = 'mysql_local')
    engine_conn = engine.get_conn()
    sql = pd.read_sql('select * from memory_used', con = engine_conn)
    df = pd.DataFrame(sql)
    df.to_csv(file, index=False)


default_args = {
    'owner' : 'airflow'
}

with DAG('dynamic_var_value',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    get_data = PythonOperator(
        task_id = 'get_data',
        python_callable = get_data
    )
    send_email = EmailOperator(
        task_id = 'send_email',
        to = Variable.get("report_user_registered"),
        cc = Variable.get("cc_email"),
        subject = "test dynamic value",
        html_content = 'ini test dynamic value',
        files = [f'{file}']
    )

    housekeeping_csv = PythonOperator(
        task_id = 'housekeeping_file',
        python_callable = housekeeping_file
    )


housekeeping_csv >> get_data >> send_email