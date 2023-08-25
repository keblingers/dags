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

now = datetime.now()
now_time = now.strftime('%Y-%m-%d %H:%M:%S')

def dag_previous_run(prev_start_date_success):
    if prev_start_date_success is not None:
        last_run = prev_start_date_success.strftime('%Y-%m-%d %H:%M:%S')
        query = f"select notification_status from notification where notification_date between '{last_run}' and '{now_time}' and notification_status = 'ERROR' "
    else:
        satu_jam = now - timedelta(hours=1)
        format_satu_jam = satu_jam.strftime('%Y-%m-%d %H:%M:%S')
        query = f"select notification_status from notification where notification_date between '{format_satu_jam}' and '{now_time}' and notification_status = 'ERROR' "
    
    source_hook = MySqlHook(mysql_conn_id='mysql_local')
    source_conn = source_hook.get_conn()
    sql = pd.read_sql(sql=query, con=source_conn)
    df = pd.DataFrame(sql)
    print(df)
    shape = df.shape[0]
    print(shape)
    if df.shape[0] == 1:
        return "send_alert"
    else:
        return "notif_is_fine"



default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('full_clean_alert_status',
    default_args=default_args,
    schedule_interval='0 * * * *',
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    get_prev_run = BranchPythonOperator(
        task_id = 'get_prev_run',
        python_callable = dag_previous_run,
        provide_context=True,
    )

    notif_error = EmailOperator(
        task_id = 'send_alert',
        to = Variable.get('to_alert_notification'),
        subject = Variable.get('alert_notification_subject'),
        html_content = Variable.get('alert_notification_content')
    )

    notif_not_error = BashOperator(
        task_id = 'notif_is_fine',
        bash_command = "echo notif is fine"
    )

get_prev_run >> [notif_error,notif_not_error]