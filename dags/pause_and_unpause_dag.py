import json
import uuid
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from shapely import wkt
from shapely.geometry import shape, MultiPolygon
from airflow import settings
from airflow.models import DagRun, DagBag, taskinstance, DagModel, DAG, TaskInstance

from services.grid_service import GridGeomDbServices
from services.json_unpack_service import ObjectExtract

session = settings.Session()


def pause_tasks(job_id):
    session.query(DagRun).filter(DagRun.run_id == job_id, DagRun.state == 'queued').update({"state": 'pause'})


def unpause_tasks(job_id):
    session.query(DagRun).filter(DagRun.run_id == job_id, DagRun.state == 'pause').update({"state": 'queued'})


def pause_unpause_job(**kwargs):

    _data = kwargs['params']
    job_id = _data.get("job_id", None)
    state =_data.get("state", None)
    if not state or state not in ['pause','unpause']:
        raise ValueError("State has to be provided either pause or unpause")
    # Pause Fetch-Reference-Grids DAG
    pause_primary_dag = session.query(DagModel).filter(DagModel.dag_id == "Fetch-Reference-Grids").first()
    pause_primary_dag.is_paused = True
    # Pause Grids-Data-Build DAG
    pause_secondary_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
    pause_secondary_dag.is_paused = True
    session.commit()
    # check if job id is provided, to perform Pause/Unpause only to that Job
    if job_id:
        get_task_conf = (session.query(DagRun).filter(DagRun.run_id == job_id).first()).conf
        spatial_dimension = get_task_conf.get('spatial_dimension',None)
        primary_job_id = get_task_conf.get('job_id',None)
        # checking if the provided job_id is of Fetch-Reference-Grids or Grids-Data-Build DAG's
        if spatial_dimension and not primary_job_id:
            get_wkt = shape(
                json.loads(json.dumps(ObjectExtract().extract_value(spatial_dimension, "geometry").get('value', None))))
            geom_wkt = get_wkt.wkt
            _geom = wkt.loads(geom_wkt)
            if _geom.geom_type == 'Polygon':
                geom_wkt = (MultiPolygon([_geom])).wkt
            # Get Global refernce grids from the geometry provided to get Grids-Data-Build Dag run id
            global_reference_grids, number_of_grids = GridGeomDbServices().get_global_refrence_grids(geom_wkt, None)
            if state == 'pause':
                # if the Job is currently in running state it needs to be stopped and paused
                dagrun = session.query(DagRun).filter(DagRun.run_id == job_id, DagRun.state == 'running').first()
                if dagrun:
                    tis = session.query(TaskInstance).filter(TaskInstance.run_id == job_id, TaskInstance.state == 'running').all()
                    for ti in tis:
                        ti.set_state(state='shutdown', session=session)
                        session.commit()
                else:
                    # if the Job is is in Queued state so it can be paused
                    pause_tasks(job_id)
                # iterate and pause all the Grids-Data-Build Dag jobs
                for i in range(len(global_reference_grids)):
                    grid_id = global_reference_grids[i].get('grid_id', None)
                    job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{job_id}_{grid_id}"))
                    pause_tasks(str(job_uuid))
                session.commit()
            elif state == 'unpause':
                # unpause the job if the Job is stopped and paused
                dagrun = session.query(DagRun).filter(DagRun.run_id == job_id,DagRun.state == 'failed').first()
                if dagrun:
                    tis = session.query(TaskInstance).filter(TaskInstance.run_id == job_id).all()
                    for ti in tis:
                        ti.set_state(state=None, session=session)
                        session.commit()
                    dagrun.state = 'running'
                    session.merge(dagrun)
                    session.commit()
                else:
                    # if the Job is is in Paused state so it can be unpaused
                    unpause_tasks(job_id)
                # iterate and unpause all the Grids-Data-Build Dag jobs
                for i in range(len(global_reference_grids)):
                    grid_id = global_reference_grids[i].get('grid_id', None)
                    job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{job_id}_{grid_id}"))
                    unpause_tasks(str(job_uuid))
                session.commit()
        else:
            # if the provided job id is of Grids-Data-Build Dag's to Pause/Unpause
            if state == 'pause':
                pause_tasks(job_id)
            elif state == 'unpause':
                unpause_tasks(job_id)

    else:
        # If the job id is not provided pause/unpause all the queued jobs
        if state == 'pause':
            session.query(DagRun).filter(DagRun.state == 'queued').update({"state": 'pause'})
        elif state == 'unpause':
            session.query(DagRun).filter(DagRun.state == 'pause').update({"state": 'queued'})
        session.commit()
    # Unpause Fetch-Reference-Grids DAG
    pause_primary_dag = session.query(DagModel).filter(DagModel.dag_id == "Fetch-Reference-Grids").first()
    pause_primary_dag.is_paused = False
    # Unpause Grids-Data-Build DAG
    pause_secondary_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
    pause_secondary_dag.is_paused = False
    session.commit()


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
        'Pause-Unpause-Job',
        default_args=default_args,
        description='A DAG to pause and unpause the job',
        schedule=None,
        start_date=days_ago(1),
        tags=['Pause-Unpause-Job'],
        params={
            "job_id":"",
            "state":"pause"
            }
) as dag:
    pause_unpause_task = PythonOperator(
        task_id='pause_unpause_job',
        depends_on_past=False,
        python_callable=pause_unpause_job,
        retries=0,
    )

    pause_unpause_task