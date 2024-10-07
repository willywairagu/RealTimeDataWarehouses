import random
from datetime import datetime, timedelta 

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pinot_schema_operator import PinotSchemaSubmitOperator 

start_date = datetime(2024, 10, 7)
default_args = {
    "owner": "wairagu",
    "depends_on_past": False,
    "start_date": start_date,
    "backfill": False
} 

with DAG(
    'schema_dag', 
    description='A DAG to submit all schema in a folder to Apache Pinot',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    tags=['schema']
) as dag:
    
    start = EmptyOperator(
        task_id='start_task'
    )

    submit_schema = PinotSchemaSubmitOperator(
        task_id='submit_schemas',
        folder_path='/opt/airflow/dags/schemas',
        pinot_url='http://pinot-controller:9000/schemas'
    )

    end = EmptyOperator(
        task_id='end_task'
    )

    start >> submit_schema >> end 
