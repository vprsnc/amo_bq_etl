import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/home/analytics/OddJob/tasks")
sys.path.insert(0, "/home/analytics/OddJob")

from events.StatusChanges import store_events
from senders import status_changes_sender


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
        'status_changes_dag',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
        )

get_status_changes = PythonOperator(
        task_id='get_status_changes',
        python_callable=store_events,
        dag=dag
        )

send_status_changes = PythonOperator(
        task_id='send_status_changes',
        python_callable=status_changes_sender,
        dag=dag
)

get_status_changes >> send_status_changes
