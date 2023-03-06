from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

def process(**kwargs):
    print("HI")
    return 'done'

with DAG(
        'Parallel-DAG',
        default_args=default_args,
        description='Parallel-DAG to migrate statistics data',
        schedule=None,
        start_date=days_ago(1),
        tags=['Parallel-DAG'],
        params={}

) as dag:
    t1 = PythonOperator(
        task_id='parallel-task',
        depends_on_past=False,
        python_callable=process,
        retries=0,
    )

    t1
        
