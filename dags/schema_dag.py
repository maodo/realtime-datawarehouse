import random
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from pinot_operator import PinotSubmitSchemaOperator

start_date = datetime(2024,11,16)

with DAG(
    dag_id='schema_dag',
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    description='Schema submit DAG',
    tags=['schema']
) as dag:
    start = EmptyOperator(task_id='start')

    submit_schema = PinotSubmitSchemaOperator(
        task_id='submit_schemas',
        folder_path='/opt/airflow/dags/schemas',
        pinot_url='http://pinot-controller:9000/schemas'
    )
    end = EmptyOperator(task_id='end')

    start >> submit_schema >> end