from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.insert(0,"/opt/airflow/")
from scripts.breweries_case import bronze_layer, silver_layer, gold_layer

version = datetime.now().strftime('%Y/%m/%d')

with DAG(
    dag_id='breweries_case',
    start_date=datetime(2024, 11, 12),
    schedule='@daily'
) as dag:
    
    extract_data = PythonOperator(task_id='extract_data', python_callable=bronze_layer, op_kwargs={'version': version})
    process_data = PythonOperator(task_id='process_data', python_callable=silver_layer, op_kwargs={'version': version})
    view_data = PythonOperator(task_id='view_data', python_callable=gold_layer, op_kwargs={'version': version})

extract_data >> process_data >> view_data
