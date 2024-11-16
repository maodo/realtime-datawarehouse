import random
from airflow import DAG
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

start_date = datetime(2024,11,16)

num_rows = 50
output_file = './customer_dim_large_data.csv'

customer_ids =[]
first_names =[]
last_names =[]
emails =[]
phone_numbers =[]
registrations =[]

def generate_random_data(num_row):
    customer_id = f'{num_row:03d}'
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.ascii_company_email()
    phone_number = fake.phone_number()

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0,365))
    registration_date_millis = int(random_date.timestamp()*1000)
    return customer_id,first_name,last_name,email,phone_number,registration_date_millis

def generate_customer_dim_data():
    row_number = 1
    while row_number < num_rows:
        customer_id,first_name,last_name,email,phone_number,registration_date_millis=generate_random_data(row_number)
        customer_ids.append(customer_id)
        first_names.append(first_name)
        last_names.append(last_name)
        emails.append(email)
        phone_numbers.append(phone_number)
        registrations.append(registration_date_millis)
        row_number+=1
    df = pd.DataFrame({
        'customer_id': customer_ids,
        'first_name': first_names,
        'last_name': last_names,
        'email': emails,
        'phone_number': phone_numbers,
        'registration_date':registrations
    })
    df.to_csv(output_file, index=False)
    print(f'CSV file  {output_file} with {num_rows} has been successfully generated')

with DAG(
     dag_id='customer_dim_data_generator',
     default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
     start_date=start_date,
     schedule_interval=timedelta(days=1),
     description='Customer Dim DAG'
    ) as dag:
     start = EmptyOperator(
            task_id='start_task'
        )
     generate_customer_dimension_data = PythonOperator(
            task_id='generate_customer_dimension_data',
            python_callable=generate_customer_dim_data
        )
     end = EmptyOperator(
            task_id='end_task'
        )
     start >> generate_customer_dimension_data >> end


