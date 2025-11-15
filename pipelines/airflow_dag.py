from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "eia",
    "start_date": datetime(2024,1,1)
}

with DAG(
    dag_id="eia_general_dataset_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
):

    ingest = BashOperator(task_id="ingest", bash_command="python ingest.py")
    transform = BashOperator(task_id="transform", bash_command="python transform.py")

    ingest >> transform
