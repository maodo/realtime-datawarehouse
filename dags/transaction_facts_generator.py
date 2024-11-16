from airflow import DAG
from airflow.operators.empty import EmptyOperator
from kafka_operator import KafkaProduceOperator
from datetime import datetime, timedelta

start_date = datetime(2024,11,16)

with DAG(
    dag_id='transaction_data_generator',
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    description='Transaction facts DAG',
    tags=['facts_data']
) as dag:
    start = EmptyOperator(
        task_id='start'
    )
    generate_transaction_data = KafkaProduceOperator(
        task_id='generate_transaction_data',
        kafka_broker='redpanda:9092',
        kafka_topic='transaction_facts',
        num_records=100
    )
    end = EmptyOperator(
        task_id='end'
    )

    start >> generate_transaction_data >> end