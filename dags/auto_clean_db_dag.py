from airflow import DAG, settings

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, DagRun, ImportError, Log, SlaMiss, RenderedTaskInstanceFields, TaskFail, \
    TaskInstance, TaskReschedule, Variable, XCom
from dotenv import load_dotenv
from airflow import AirflowException
from services.db_service import Session
from models.datafactory import Statistics, Dataset, Validations
from datetime import timedelta
import os


file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))

AUTO_CLEAN_DB_IN_DAYS = int(os.getenv("AUTO_CLEAN_DB_IN_DAYS", 90))

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

OBJECTS_TO_CLEAN = [[BaseJob,BaseJob.latest_heartbeat],
    [DagModel,DagModel.last_parsed_time],
    [DagRun,DagRun.execution_date],
    [ImportError,ImportError.timestamp],
    [Log,Log.dttm],
    [SlaMiss,SlaMiss.execution_date],
    [RenderedTaskInstanceFields,RenderedTaskInstanceFields.execution_date],
    [TaskInstance, TaskInstance.execution_date],
    [TaskReschedule, TaskReschedule.execution_date],
    [XCom,XCom.execution_date]]

DF_OBJECTS_TO_CLEAN = [[Validations,Validations.created_at],[Statistics,Statistics.created_at],
                       [Dataset,Dataset.created_at]]

def cleanup_db_fn(**kwargs):
    _data = kwargs['params']
    max_db_storage_days = _data.get("max_db_storage_days", AUTO_CLEAN_DB_IN_DAYS)
    #if max_db_storage_days <= 30:
        #raise AirflowException("Can't delete data less than 30 days")
    session = settings.Session()
    df_session = Session()

    oldest_date = days_ago(int(max_db_storage_days))
    print("oldest_date: ", oldest_date)

    for obj in OBJECTS_TO_CLEAN:
        query = session.query(obj[0]).filter(obj[1] <= oldest_date)
        print(str(obj[0]), ": ", str(len(query.all())))
        query.delete(synchronize_session=False)
    session.commit()

    for df_obj in DF_OBJECTS_TO_CLEAN:
        df_query = df_session.query(df_obj[0]).filter(df_obj[1] <= oldest_date)
        print(str(df_obj[0]), ": ", str(len(df_query.all())))
        df_query.delete(synchronize_session=False)
        df_session.commit()

    return "OK"


with DAG(
        'Auto-Clean-DB',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@quarterly',
        tags=['db-clean'],
        params={
                "max_db_storage_days": AUTO_CLEAN_DB_IN_DAYS
                }
) as dag:
    cleanup_db = PythonOperator(
        task_id="cleanup_db",
        python_callable=cleanup_db_fn,
        provide_context=True
    )

    cleanup_db
