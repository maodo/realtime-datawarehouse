import random
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

start_date = datetime(2024,11,16)

num_rows = 50
output_file = './branch_dim_large_data.csv'


branch_ids = []
branch_names = []
branch_addresses = []
cities_ = []
states = []
opening_dates = []
zip_codes = []

cities = ["London","Manchester","Birmingham","Glasgow","Edinburgh"]
regions = ["London","Greater Manchester","West Midlands","Scotland","Scotland"]
postcodes = ["EC1A 1BB","M1 1AE","G1 1AA","B1 1AA","EH1 1AA"]

def generate_random_data(num_row):
    branch_id = f'A{num_row:04d}'
    branch_name = f'Branch {num_row:}'
    branch_address = f'{random.randint(1,999)} {random.choice(["High St","King St","Queen St","Church Rd"])}'
    city = random.choice(cities)
    region = random.choice(regions)
    postcode = random.choice(postcodes)

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0,365))
    opening_date_millis = int(random_date.timestamp()*1000)
    return branch_id,branch_name,branch_address,city,region,postcode,opening_date_millis

def generate_branch_dim_data():
    row_num = 1
    while row_num < num_rows:
        branch_id,branch_name,branch_address,city,region,postcode,opening_date_millis = generate_random_data(row_num)
        branch_ids.append(branch_id)
        branch_names.append(branch_name)
        branch_addresses.append(branch_address)
        cities_.append(city)
        states.append(region)
        zip_codes.append(postcode)
        opening_dates.append(opening_date_millis)
        row_num+=1
    df = pd.DataFrame({
        'branch_id': branch_ids,
        'branch_name': branch_names,
        'branch_address':branch_addresses,
        'city': cities_,
        'state': states,
        'zipcode': zip_codes,
        'opening_date': opening_dates
    })
    df.to_csv(output_file,index=False)
    print(f'CSV file  {output_file} with {num_rows} has been successfully generated')

with DAG(
     dag_id='branch_dim_generator',
     default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
     description='Branch Dim DAG',
     schedule_interval=timedelta(days=1),
     start_date=start_date
    ) as dag:
    start= EmptyOperator(
            task_id='start_task'
        )
    generate_branch_dimension_data = PythonOperator(
            task_id='generate_branch_dimension_data',
            python_callable= generate_branch_dim_data
        )
    end =  EmptyOperator(
            task_id='end_task'
        )

    start >> generate_branch_dimension_data >> end