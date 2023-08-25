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
import yfinance as yf
import telegram_send

def get_buy_data():
    db_hook = MySqlHook(mysql_conn_id = 'mysql_ihsg')
    db_conn = db_hook.get_conn()
    db_table = 'stock_buy'
    sql = pd.read_sql(f"select * from {db_table}",con=db_conn)
    df = pd.DataFrame(sql)
    db_to_json = df.to_json(orient='records',date_format='iso',date_unit='us')

    return db_to_json

def get_price():
    buy_data = pd.read_json(get_buy_data())
    #print(buy_data)
    #telegram_send.send(messages=[buy_data])
    lists = Variable.get("stocks")
    stocks = lists.split(",")
    curprice = []
    for x in stocks:
        data = buy_data.query("stock_buy_name == @x")
        buy_quantity = data['stock_buy_quantity'].iloc[0]
        buy_price = data['stock_buy_price'].iloc[0]
        total_buy = buy_price * buy_quantity
        current_price = yf.Ticker(x).info['currentPrice']
        total_current_price = current_price * buy_quantity
        if buy_price == 0:
            gl = '-'
        elif current_price > buy_price:
            gl = 'G'
        elif current_price < buy_price:
            gl = 'L'
        gl_amount = total_current_price - total_buy
        price = f'{x} = {current_price} -> ({gl}) {gl_amount}'
        #curprice.append(price)
        telegram_send.send(messages=[price])
    #telegram_send.send(messages=[curprice])


default_args = {
    'owner' : 'airflow',
    'email' : ['bsotoayam@gmail.com'],
    'email_on_failure' : False
}

with DAG('stock_price',
    default_args=default_args,
    schedule_interval='0 3-9 * * 1-5',
    start_date=datetime(2022,4,6),
    catchup=False
) as dag:

    buy_data = PythonOperator(
        task_id = 'get_buy_data',
        python_callable = get_buy_data
    )

    stock_price = PythonOperator(
        task_id = 'get_stock_price',
        python_callable = get_price
    )

buy_data >> stock_price