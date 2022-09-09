emport sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/home/analytics/OddJob/tasks")
sys.path.insert(0, "/home/analytics/OddJob")

from event import get_events, store_events, cleanup_events
from senders import events_sender


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
        'status_dag',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
        )

get_events = PythonOperator(
        task_id='get_events',
        python_callable=store_events,
        dag=dag
        )

send_events = PythonOperator(
        task_id='send_events',
        python_callable=events_sender,
        dag=dag
)

cleanup = PythonOperator(
    task_id='clean_up_events',
    python_callable=cleanup_events,
    dag=dag
)

get_events >> send_events >> cleanup
