
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# custome modes for storing data
from models.datafactory import Statistics
from models.config import sample_batch_jobs_template
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import airflow.settings
import time
from airflow.models import DagRun
from airflow.utils import timezone

session = airflow.settings.Session()


def trigger_fetch_grids_dag(**kwargs):
    # get the configuration for job
    data = kwargs['params']
    job_id = kwargs['dag_run'].run_id if kwargs['dag_run'] else 'test_multiple_years_data_build_dag'
    start_year = data.get('start_year', None)
    end_year = data.get('end_year', None)
    template = data.get('template', None)

    if start_year and end_year:
        for year in range(int(start_year), int(end_year) + 1):
            # check if the data build for year already processed or not
            last_processed_year = Variable.get(job_id, None)
            if last_processed_year and int(year) <= int(last_processed_year):
                continue
            else:
                # If the Dag is paused clear the current running Job
                if dag.get_is_paused():
                    dag.clear(only_running=True, dag_run_state='running', confirm_prompt=False)
                    time.sleep(10)
                current_time = time.time()
                parsed_execution_date = timezone.utcnow()
                # update year in Temporal Dimension Object
                for obj in template['temporal_dimension']:
                    if obj.get('type',None) == "YEAR":
                        obj.update(value=year)

                # Temporarily Storing the year that is processed by current Job
                Variable.set(job_id, year)
                # Trigger Fetch grids Dag to process
                TriggerDagRunOperator(
                    task_id=f'trigger-fetch-grids-dag_{current_time}',
                    trigger_dag_id="Fetch-Reference-Grids",
                    wait_for_completion=True,
                    conf=template,
                    execution_date=parsed_execution_date,
                    poke_interval = 60,
                    allowed_states=['success','failed'],
                    dag=dag).execute(context=kwargs)
                check_grids_data_build_dag = session.query(DagRun).filter(DagRun.dag_id == "Grids-Data-Build"). \
                                             filter(DagRun.state == "running").first()
                # wait until all the Grids data build jobs are completed
                while check_grids_data_build_dag:
                    check_grids_data_build_dag = session.query(DagRun).filter(DagRun.dag_id == "Grids-Data-Build"). \
                                                 filter(DagRun.state == "running").first()
                    time.sleep(60)

        Variable.delete(job_id)
    return "OK"


# These args will get passed on to each operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'Multiple-Years-Data-Build',
        default_args=default_args,
        description='A Multiple Years Data Build DAG',
        schedule=None,
        start_date=days_ago(1),
        tags=['Multiple-Years-Data-Build'],
        params=sample_batch_jobs_template,

) as dag:
    t1 = PythonOperator(
        task_id='trigger_fetch_grids_dag',
        depends_on_past=False,
        python_callable=trigger_fetch_grids_dag,
        retries=0,
    )

    t1
