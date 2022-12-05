import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import datetime
from yt_etl import run_yt_etl

with DAG(
    "track_mood",
    default_args = {
        'depends_on_past': False,
        'email': ['danielliao66@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    description='',
    schedule=datetime.timedelta(days=7),
    start_date=datetime.datetime(2022, 12, 4),
    catchup=False,
    tags=['mood_tracker'],
) as dag:
    mood_tracker = PythonOperator(
        task_id="mood_tracker",
        python_callable = run_yt_etl,
        dag=dag
    )
    ready = DummyOperator(task_id="ready")
    mood_tracker >> ready