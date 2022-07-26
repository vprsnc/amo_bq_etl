import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/home/analytics/OddJob/tasks")
sys.path.insert(0, "/home/analytics/OddJob")

from leads_etl import run_leads_etl


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(0, 0, 0, 0),
        'email': ['thegeorgy@icloud.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=1)
        }

dag = DAG(
        'leads_dag',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
        )

run_task = PythonOperator(
        task_id='leads_etl',
        python_callable=run_leads_etl,
        dag=dag
        )

run_task
