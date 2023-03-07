import json
import uuid
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.base import Base
from shapely import wkt
from shapely.geometry import shape, MultiPolygon
from sqlalchemy import Table
from airflow import settings
from airflow.models import DagRun, DagBag, taskinstance, DagModel, DAG, TaskInstance
from services.db_service import Session
from services.grid_service import GridGeomDbServices
from services.json_unpack_service import ObjectExtract
from models.datafactory import Statistics, Dataset, Validations

session = settings.Session()
df_session = Session()


class DeleteJob:

    def delete_tasks(self, job_id):
        dagrun = session.query(DagRun).filter(DagRun.run_id == str(job_id))
        if dagrun.first():
            ti = session.query(TaskInstance).filter(
                TaskInstance.execution_date == dagrun.first().execution_date)
            ti.delete(synchronize_session='fetch')
            dagrun.delete(synchronize_session='fetch')
            session.commit()


def delete_job_data(**kwargs):
    """
    Delete reference data from DB and from S3
    """
    _data = kwargs['params']
    job_id = _data.get("job_id", None)

    if job_id:
        # Delete all Grids-Data-Build Dag jobs which got loaded by Fetch-Reference-Grids Dag
        get_task_conf = (session.query(DagRun).filter(DagRun.run_id == job_id).first()).conf
        spatial_dimension = get_task_conf.get('spatial_dimension',None)
        primary_job_id = get_task_conf.get('job_id',None)
        if spatial_dimension and not primary_job_id:
            # Pause Grids-Data-Build Dag Jobs
            pause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
            pause_dag.is_paused = True
            session.commit()

            get_wkt = shape(
                json.loads(json.dumps(ObjectExtract().extract_value(spatial_dimension, "geometry").get('value', None))))
            geom_wkt = get_wkt.wkt
            _geom = wkt.loads(geom_wkt)
            if _geom.geom_type == 'Polygon':
                geom_wkt = (MultiPolygon([_geom])).wkt
            # Get Global refernce grids from the geometry provided to get Grids-Data-Build Dag run id
            global_reference_grids, number_of_grids = GridGeomDbServices().get_global_refrence_grids(geom_wkt, None)
            DeleteJob().delete_tasks(job_id)
            for i in range(len(global_reference_grids)):
                grid_id = global_reference_grids[i].get('grid_id', None)
                job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{job_id}_{grid_id}"))
                DeleteJob().delete_tasks(str(job_uuid))
            # Unpause Grids-Data-Build Dag
            unpause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
            unpause_dag.is_paused = False
            session.commit()
        # Delete DagRun and TaskInstance from any Dag
        DeleteJob().delete_tasks(job_id)
        # delete validations saved in DB associated with job id
        df_session.query(Validations).filter(Validations.job_id == job_id).delete()
        df_session.commit()
        # delete Statistics saved in DB associated with job id
        df_session.query(Statistics).filter(Statistics.job_id == job_id).delete()
        df_session.commit()
        # delete Dataset saved in DB associated with job id
        df_session.query(Dataset).filter(Dataset.job_id == job_id).delete()
        df_session.commit()
        return "OK"

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
        'Delete-Job-Data',
        default_args=default_args,
        description='Delete-Data associated with Job ID DAG',
        schedule=None,
        start_date=days_ago(1),
        tags=['Delete-Job-Data'],
        params={
            "job_id": ""
        }

) as dag:
    t1 = PythonOperator(
        task_id='delete_job_data',
        depends_on_past=False,
        python_callable=delete_job_data,
        retries=0,
    )

    t1
