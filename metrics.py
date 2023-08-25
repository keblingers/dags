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
from datetime import datetime



def test():
    #db_list = ['sabi_company','sabi_inventory','sabi_finance','sabi_marketing','sabi_note','sabi_notification','sabi_sales','sabi_user']
    db_list = 'indonesia,airflow,flyway,locking,coba'
    #db_name = str(db_list).split(",")
    db_name = str(db_list).split(",")
    now = datetime.now()
    #period = now.strftime('%Y-%m')
    period = '2022-06'
    db_host = 'alimysql'
    #print(db_name)
    data = []
    for x in db_name:
        #print(f'db_name = {x} db_hostname = {db_host}')
        #print(x)
        engine = MySqlHook(mysql_conn_id = 'mysql_report')
        conn = engine.get_conn()
        query = f"""select (select total_allocated from memory_used where hostname = '{db_host}' 
        and memory_used_date like '{period}%' and database_name = '{x}' order by memory_used_date asc limit 1)
        as awal_bulan, (select total_allocated from memory_used where hostname = '{db_host}' and 
        memory_used_date like '{period}%' and database_name = '{x}' order by memory_used_date desc limit 1) 
        as akhir_bulan;"""
        # query = f"""select database_size_in_mb from database_size where database_size_hostname = '{db_host}' 
        # and database_size_date like '{period}%' and database_size_name = '{x}' order by database_size_date asc limit 1"""
        #print(query)
        sql = pd.read_sql(query,con=conn)
        df = pd.DataFrame(sql)
        #print(df)
        database_name = f'{x}'
        df['growth'] = df['akhir_bulan'] - df['awal_bulan']
        df['database_name'] = database_name
        df = df[["database_name","awal_bulan","akhir_bulan","growth"]]
        data.append(df)
    data = pd.concat(data)
    print(data)


default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('testing_dag',
    default_args=default_args,
    schedule_interval='0 3-9 * * 1-5',
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    test = PythonOperator(
        task_id = 'testing',
        python_callable=test
    )

test