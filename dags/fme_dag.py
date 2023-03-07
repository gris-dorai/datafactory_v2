import os
import uuid
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import settings
from airflow.models import DagRun
import time
from airflow.utils import timezone
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import Variable
from airflow import AirflowException
import requests
from dotenv import load_dotenv, find_dotenv
from urllib.parse import quote

load_dotenv(find_dotenv())

session = settings.Session()


def check_grids_data_build_dag_task_status(secondary_job_ids):
    check_grids_data_build_dag = session.query(DagRun).filter(DagRun.run_id.in_(secondary_job_ids)). \
        filter(DagRun.dag_id == "Grids-Data-Build", DagRun.state == "running").first()

    while check_grids_data_build_dag:
        check_grids_data_build_dag = session.query(DagRun).filter(DagRun.run_id.in_(secondary_job_ids)). \
            filter(DagRun.dag_id == "Grids-Data-Build", DagRun.state == "running").first()
        # wait until all the Fetch-Reference-Grids Dag jobs are completed
        time.sleep(30)

    check_failed_secondary = session.query(DagRun).filter(DagRun.run_id.in_(secondary_job_ids)). \
        filter(DagRun.dag_id == "Grids-Data-Build", DagRun.state == "failed").first()
    if check_failed_secondary:
        return True
    else:
        return False


def trigger_retry_dag(retry_job_uuid):
    current_time = time.time()
    execution_date = timezone.utcnow()
    run_id = str(f'{retry_job_uuid}_{current_time}')
    trigger_dag(dag_id='Retry-Failed-Tasks', run_id=run_id, execution_date=execution_date,
                replace_microseconds=False, conf={})
    check_retry_failed_task = session.query(DagRun).filter(DagRun.run_id == run_id). \
        filter(DagRun.dag_id == "Retry-Failed-Tasks", DagRun.state == "success").first()

    while not check_retry_failed_task:
        check_retry_failed_task = session.query(DagRun).filter(DagRun.run_id == str(f'{retry_job_uuid}_{current_time}')). \
            filter(DagRun.dag_id == "Retry-Failed-Tasks", DagRun.state == "success").first()
        # wait until all the Retry-Failed-Tasks Dag task are completed
        time.sleep(30)


def retry_failed_tasks(fme_job_id, secondary_job_ids):
    check_tasks_status = check_grids_data_build_dag_task_status(secondary_job_ids)
    if check_tasks_status:
        retry_job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{fme_job_id}"))
        get_reties = Variable.get(fme_job_id, None)
        if not get_reties:
            Variable.set(fme_job_id, 1)
            trigger_retry_dag(retry_job_uuid)
            retry_failed_tasks(fme_job_id, secondary_job_ids)
        elif int(get_reties) > 2:
            Variable.delete(fme_job_id)
            raise AirflowException("Job retries limit exceeded")
        else:
            Variable.set(fme_job_id, int(get_reties) + 1)
            trigger_retry_dag(retry_job_uuid)
            retry_failed_tasks(fme_job_id, secondary_job_ids)
    else:
        return False


def trigger_fme(**kwargs):

    data = kwargs['params']
    primary_job_id = data.get('primary_job_id', None)
    dataset_id = data.get('dataset_id', None)
    fme_params = data.get('fme_params', None)
    provider = data.get('provider', None)
    fme_token = str(os.getenv("FME_TOKEN", None))
    publishing_api_url = str(os.getenv("PUBLISHING_API_URL", None))
    data_product_unique_id = data.get('data_product_unique_id', None)
    global_reference_grids = data.get('global_reference_grids',None)
    fme_job_id = kwargs['dag_run'].run_id if kwargs['dag_run'] else 'test_fme_dag'
    secondary_job_ids =[]
    for i in range(len(global_reference_grids)):
        grid_id = global_reference_grids[i].get('grid_id', None)
        job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{primary_job_id}_{grid_id}"))
        secondary_job_ids.append(str(job_uuid))
    if not retry_failed_tasks(fme_job_id, secondary_job_ids):
        api_url = fme_params.get('api_url',None)
        destination_db = fme_params.get('destination_db',None)
        destination_table = fme_params.get('destination_table', None)
        publishing_api_params = fme_params.get('publishing_api_params', None)
        source_db = fme_params.get('source_database', 'DF_dev')
        if data_product_unique_id:
            data_product_unique_id = f'&data_product_unique_id={data_product_unique_id}'
        else:
            data_product_unique_id = f'&data_product_unique_id='
        fme_url = f'{api_url}destination_table_name={destination_table}&DestDataset_POSTGRES={destination_db}&' \
                  f'job_id={quote(primary_job_id)}&dataset_id={dataset_id}&publishing_api_params={publishing_api_params}&' \
                  f'opt_showresult=false&logs_db={destination_db}&source_database={source_db}{data_product_unique_id}' \
                  f'&opt_servicemode=sync&opt_responseformat=json&provider={provider}&publishing_api_url={publishing_api_url}'
        headers = {
            'Content-type': 'application/json',
            'Authorization': fme_token
        }
        try:
            response = requests.post(fme_url, headers=headers)
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        print(response.text)
        print(response.status_code)
        if response.status_code != 200:
            raise AirflowException("FME Trigger Failed")


# These args will get passed on to each operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
        'Trigger-FME',
        default_args=default_args,
        description='Trigger FME to migrate statistics data',
        schedule=None,
        start_date=days_ago(1),
        tags=['Trigger-FME'],
        params={
            "job_id": ""
        }

) as dag:
    t1 = PythonOperator(
        task_id='trigger_fme',
        depends_on_past=False,
        python_callable=trigger_fme,
        retries=0,
    )

    t1
