from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
from urllib.parse import urlencode

start_date = datetime(2024,11,16)
BASE_URL = "http://pinot-controller:9000/ingestFromFile"
BATCH_CONFIG_MAP_STR = '{"inputFormat":"csv","recordReader.prop.delimiter":","}'
# Mapping of tables to their respective files
TABLE_FILE_MAPPING = {
    "account_dim_OFFLINE": "/opt/airflow/account_dim_large_data.csv",
    "customer_dim_OFFLINE": "/opt/airflow/customer_dim_large_data.csv",
    "branch_dim_OFFLINE": "/opt/airflow/branch_dim_large_data.csv",
}
encoded_batch_config_map_str = urlencode({"batchConfigMapStr": BATCH_CONFIG_MAP_STR})

with DAG(
    dag_id='import_dims_data',
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    start_date=start_date,
    schedule_interval='@daily',
    catchup=False,
    description='Load dimensions data DAG',
    tags=['dimensions','load','data']
) as dag:
    for table_name, file_path in TABLE_FILE_MAPPING.items():
        # Encoded URL
        url = f"{BASE_URL}?tableNameWithType={table_name}&{encoded_batch_config_map_str}"
        # Bash task for ingestion
        task = BashOperator(
            task_id=f"ingest_{table_name}",
            bash_command=(
                f"curl -X POST -F file=@{file_path} "
                f"-H 'Content-Type: multipart/form-data' "
                f"'{url}'"
            )
        )
    # start = EmptyOperator(task_id='start')
    # ingest_account_dim = BashOperator(
    #     task_id='ingest_account_dim',
    #     bash_command=(
    #         'curl -X POST -F file=@/opt/airflow/account_dim_large_data.csv ' 
    #         '-H "Content-Type: multipart/form-data" '
    #         f"http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&{encoded_batch_config_map_str}"
    #     ))
    # ingest_customer_dim = BashOperator(
    #     task_id='ingest_customer_dim',
    #     bash_command=(
    #         'curl -X POST -F file=@/opt/airflow/customer_dim_large_data.csv ' 
    #         '-H "Content-Type: multipart/form-data" '
    #         f"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&{encoded_batch_config_map_str}"
    #     ))
    # ingest_branch_dim = BashOperator(
    #     task_id='ingest_branch_dim',
    #     bash_command=(
    #         'curl -X POST -F file=@/opt/airflow/branch_dim_large_data.csv ' 
    #         '-H "Content-Type: multipart/form-data" '
    #         f"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&{encoded_batch_config_map_str}"
    #     ))
    # end = EmptyOperator(task_id='end')

    # start >> ingest_account_dim >> ingest_customer_dim >> ingest_branch_dim >> end

    # curl -X POST -F file=@/opt/airflow/account_dim_large_data.csv -H "Content-Type: multipart/form-data" "http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%7C%22%7D"