import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/home/analytics/OddJob/tasks")
sys.path.insert(0, "/home/analytics/OddJob")

from leads import store_leads, cleanup_leads
from senders import leads_sender


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

get_leads = PythonOperator(
    task_id='get_leads',
    python_callable=store_leads,
    dag=dag
)

send_leads = PythonOperator(
    task_id='send_leads',
    python_callable=leads_sender,
    dag=dag
)

cleanup = PythonOperator(
    task_id = 'clean_up_leads',
    python_callable=cleanup_leads,
    dag=dag
)

get_leads >> send_leads >> cleanup
