import random
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from pinot_table_operator import PinotSubmitTableOperator

start_date = datetime(2024,11,16)

with DAG(
    dag_id='table_dag',
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    description='Table submit DAG to Pinot',
    tags=['table']
) as dag:
    start = EmptyOperator(task_id='start')

    submit_table = PinotSubmitTableOperator(
        task_id='submit_tables',
        folder_path='/opt/airflow/dags/tables',
        pinot_url='http://pinot-controller:9000/tables'
    )
    end = EmptyOperator(task_id='end')

    start >> submit_table >> end