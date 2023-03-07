from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


templated_command = """
    echo yes | airflow tasks clear -s 2020-01-01 -e 2028-05-01 --only-failed "{{ params.my_param }}"
"""

# These args will get passed on to each operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
    'Retry-Failed-Tasks',
    default_args=default_args,
    description='A DAG to retry failed tasks',
    schedule=None,
    start_date=days_ago(1),
    tags=['Retry-Failed-Tasks'],
) as dag:

    retry_task = BashOperator(
        task_id='restart_failure_tasks',
        bash_command=templated_command,
        params={'my_param': 'Grids-Data-Build'},
    )

    retry_task