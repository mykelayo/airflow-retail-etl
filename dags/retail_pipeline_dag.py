from datetime import datetime
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from etl_pipeline import run_etl

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
}

dag = DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='retail ETL pipeline',
    schedule='@daily',  # Run daily
    catchup=False
)

# Define the task
etl_task = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl,
    dag=dag
)

# Set task dependencies (single task in this case)
etl_task