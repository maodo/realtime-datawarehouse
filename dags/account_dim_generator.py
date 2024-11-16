import random
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

start_date = datetime(2024,11,16)

num_rows = 50
output_file = './account_dim_large_data.csv'

account_ids = []
account_types = []
opening_dates = []
statuses = []
customer_ids = []
balances = []

def generate_random_data(num_row):
    account_id = f'A{num_row:05d}'
    account_type = random.choice(['SAVINGS','CHECKS'])
    status = random.choice(['ACTIVE','INACTIVE'])
    customer_id = f'{random.randint(1,1000)}:05d'
    balance = round(random.uniform(100.0, 10000.00),2)
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0,365))
    opening_date_millis = int(random_date.timestamp()*1000)
    return account_id, account_type, status, customer_id, balance,opening_date_millis

def generate_account_dim_data():
    row_num = 1
    while row_num < num_rows:
        account_id, account_type, status, customer_id, balance,opening_date_millis = generate_random_data(row_num)
        account_ids.append(account_id)
        account_types.append(account_type)
        customer_ids.append(customer_id)
        balances.append(balance)
        opening_dates.append(opening_date_millis)
        statuses.append(status)
        row_num+=1
    df = pd.DataFrame({
        'account_id': account_ids,
        'account_type': account_types,
        'customer_id': customer_ids,
        'opening_date': opening_dates,
        'status':statuses,
        'balance': balances,
    })
    df.to_csv(output_file,index=False)
    print(f'CSV file  {output_file} with {num_rows} has been successfully generated')
    
with DAG(
     'account_dim_generator', 
     default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
     description='Account Dim DAG',
     schedule_interval=timedelta(days=1),
     start_date=start_date,
     tags=['schema']) as dag:
      start= EmptyOperator(
            task_id='start_task'
        )
      generate_account_dimension_data = PythonOperator(
            task_id='generate_account_dimension_data',
            python_callable= generate_account_dim_data
        )
      end =  EmptyOperator(
            task_id='end_task'
        )
      start >> generate_account_dimension_data >> end 
    
    