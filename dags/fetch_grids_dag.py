import functools
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException
from airflow.exceptions import AirflowFailException
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from shapely import wkt
from shapely.geometry import shape
from shapely.geometry.multipolygon import MultiPolygon

from services.data_product_query_service import PROVIDERSCHEMALOOKUP
from services.provider_service import PROVIDERLOOKUP
from services.db_service import Session
from services.market_facts_service import exclude
from services.json_unpack_service import ObjectExtract, UnPackObj
from services.grid_service import GridGeomDbServices
# custome modes for storing data
from models.datafactory import Dataset, Statistics, ProviderVariables, Providers
from models.config import sample_template
from models.datafactory import MarketFacts
from airflow.models import Variable
import airflow.settings
from airflow.models import DagModel
import time
from airflow.models import DagRun
from airflow.utils import timezone
#from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.api.common.trigger_dag import trigger_dag
import boto3
import botocore
import pandas as pd
import io
import requests

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))

# static variables setup
NUMBER_OF_GRIDS_TO_SAVE = os.getenv("NUMBER_OF_GRIDS_TO_SAVE", None)
GLOBAL_REFRENCE_API = os.getenv("GLOBAL_REFRENCE_API", None)
MAX_ACTIVE_DAGS = int(os.getenv("MAX_ACTIVE_DAGS", None))
AIRFLOW_REST_URL = os.getenv("AIRFLOW_REST_URL", None)
MAX_FAIL_DAG = os.getenv("MAX_FAIL_DAG", None)
MAX_WORKERS = os.getenv("MAX_WORKERS", None)
GRIS_CROP_CODE_NAMES = os.getenv("GRIS_CROP_CODE_NAMES", None)
AWS_ACCCESS_KEY_ID = str(os.getenv("AWS_ACCCESS_KEY_ID", None))
AWS_SECRET_ACCESS_KEY = str(os.getenv("AWS_SECRET_ACCESS_KEY", None))
BUCKET = str(os.getenv("BUCKET", None))
PUBLISHING_METADATA_VALIDATE_API = str(os.getenv("PUBLISHING_METADATA_VALIDATE_API", None))
PUBLISHING_API_KEY = str(os.getenv("PUBLISHING_API_KEY", None))

session = airflow.settings.Session()
df_session = Session()

'''
# commenting the logic to unpack json provided in the template
class UnpackJson:

    def check_list(self,json_obj):
        list_obj = True
        for v in json_obj:
            if isinstance(v, dict):
                list_obj = False
            elif isinstance(v, list):
                self.check_list(v)
            else:
                list_obj = True
        return list_obj

    def json_extract(self,obj):
        """Recursively fetch values from nested JSON."""
        data_list = {}

        def extract(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, list):
                        if self.check_list(v):
                            data_list[f"{k}"] = v
                    if isinstance(v, (dict, list)):
                        extract(v)
                    else:
                        data_list[f"{k}"] = v

            elif isinstance(obj, list):
                for item in obj:
                    extract(item)
            return data_list

        values = extract(obj)
        return values
'''

def trigger_task(global_reference_grids,name,provider,provider_params,job_id,validate,context_code,measure_dimension,
                 temporal_dimension,product_dimension,spatial_dimension,dataset_id,irrigated_dataset_id,
                 non_irrigated_dataset_id,crop):
    """
    Trigger Grids Data Build Dag with required config details to process
    """
    index_no = global_reference_grids[0]
    grid_id = global_reference_grids[1].get('grid_id', None)
    job_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{job_id}_{grid_id}"))
    
    # check_already_processed = DagRun.find(run_id=str(job_uuid))
    check_already_processed = session.query(DagRun).filter(DagRun.run_id == str(job_uuid)).first()
    if not check_already_processed:
        execution_date = timezone.utcnow()
        trigger_dag(dag_id='Grids-Data-Build', run_id=str(f'{job_uuid}'),execution_date=execution_date,
                    replace_microseconds=False,
                    conf={"name": f'{name}_{index_no}', "provider": provider, "provider_params": provider_params,
                          "geometry": global_reference_grids[1], "job_id": job_id, "validate": validate,
                          "context_code": context_code, "measure_dimension": measure_dimension,
                          "temporal_dimension": temporal_dimension, "product_dimension": product_dimension,
                          "spatial_dimension": spatial_dimension,
                          "dataset_list": [{'dataset_id': dataset_id, 'type': 'generic'},
                                           {'dataset_id': irrigated_dataset_id, 'type': 'irrigated'},
                                           {'dataset_id': non_irrigated_dataset_id,'type': 'non-irrigated'}]})


def get_global_reference_grids(**kwargs):
    """
    Dag to get grids from Grids table passed params
    """
    # get the configuration for job
    data = kwargs['params']
    job_id = kwargs['dag_run'].run_id if kwargs['dag_run'] else 'test_fetch_grids_dag'

    name = data.get('name', None)
    provider = data.get('provider', None)
    provider_params = data.get('provider_params', None)
    temporal_dimension = data.get('temporal_dimension', None)
    measure_dimension = data.get('measure_dimension', None)
    product_dimension = data.get('product_dimension', None)
    spatial_dimension = data.get('spatial_dimension', None)
    validate = data.get('validate', None)
    context_code = data.get('context_code', None)
    # get task_state key value from provided config, if value provided as fail then fail Grids-Data-Build dag
    task_status = data.get('task_state', None)
    wait_time = data.get('provider_params',None).get('wait_time',0)
    data_product_unique_id = data.get('data_product_unique_id', None)
    trigger_fme = data.get('trigger_fme', None)
    fme_params = data.get('fme_params', None)
    temporal_dimension = list(filter(lambda x: x["type"] not in ["END_YEAR", "END_DATE","START_DATE"], temporal_dimension))
    try:
        PROVIDERLOOKUP[provider.lower()](provider).validate_template(data)
    except KeyError:
        raise ValueError("Provider Not Supported")

    if trigger_fme:
        headers = {
            'Content-type': 'application/json',
        }
        payload = data.get('fme_params', None).get('publishing_api_params', None)
        publishing_api_url = f'{PUBLISHING_METADATA_VALIDATE_API}?apikey={PUBLISHING_API_KEY}'
        try:
            response = requests.post(publishing_api_url, json=payload, headers=headers)
        except Exception as e:
            raise AirflowException(f"api failure: {e}")

        if not response.json().get('status', None) == 1:
            raise AirflowException(f"Publishing API metadata validation failed: {response.json().get('error', None)}")

    crop = ObjectExtract().extract_value(product_dimension, "CROP").get('value', None)

    #Pause Jobs in Grids-Data-Build Dag
    pause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
    pause_dag.is_paused = True
    session.commit()
    provider_obj = df_session.query(Providers).filter(Providers.name == provider.lower()).first()
    if provider_obj:
        concurrency = int(provider_obj.concurrency)
        Variable.set('max_active_tasks', concurrency)
        pause_dag.concurrency = concurrency
    df_session.commit()
    region = ObjectExtract().extract_value(spatial_dimension, "REGION").get('value',None)
    year = ObjectExtract().extract_value(temporal_dimension, "YEAR").get('value',None)
    # store job_id and its state. which can be used in Grids-Data-Build dag to fail task.
    if task_status and task_status == "fail":
        Variable.set(job_id, "fail")

    # convert provided polygon to wkt
    get_wkt = shape(json.loads(json.dumps(ObjectExtract().extract_value(spatial_dimension, "geometry").get('value',None))))
    geom_wkt = get_wkt.wkt
    _geom = wkt.loads(geom_wkt)
    if _geom.geom_type == 'Polygon':
        geom_wkt = (MultiPolygon([_geom])).wkt

    # psql to fetch intersecting grids
    total_global_reference_grids, number_of_grids = GridGeomDbServices().get_global_refrence_grids(geom_wkt,year)
    if not total_global_reference_grids:
        raise AirflowException("Failed to fetch global reference grids")

    global_reference_grids = []
    for idx, gridcell in enumerate(total_global_reference_grids):
        if not exclude(gridcell, crop, product_dimension):
            global_reference_grids.append(dict(gridcell))

    irrigation = ObjectExtract().extract_value(product_dimension, "IRRIGATION").get('value',None)
    # Creating Dataset Object
    if irrigation and not isinstance(irrigation,list) and irrigation.lower() == "irrigated":
        dataset_id =None
        check_dataset = df_session.query(Dataset).filter(Dataset.job_id == job_id).order_by(Dataset.created_at.asc()).all()
        if check_dataset:
            irrigated_dataset_id = str(check_dataset[0].dataset_id)
            non_irrigated_dataset_id = str(check_dataset[1].dataset_id)
        else:
            irrigated_dataset = Dataset(job_id=str(job_id),region=region,year=year, crop=crop,feature_geom=geom_wkt,
                                        data_product_unique_id=data_product_unique_id,created_at=datetime.now())
            df_session.add(irrigated_dataset)
            non_irrigated_dataset = Dataset(job_id=str(job_id),region=region,year=year, crop=crop,feature_geom=geom_wkt,
                                            data_product_unique_id=data_product_unique_id,created_at=datetime.now())
            df_session.add(non_irrigated_dataset)
            df_session.commit()
            irrigated_dataset_id = str(irrigated_dataset.dataset_id)
            non_irrigated_dataset_id = str(non_irrigated_dataset.dataset_id)
    else:
        irrigated_dataset_id = non_irrigated_dataset_id = None
        check_dataset = df_session.query(Dataset).filter(Dataset.job_id == job_id).first()
        if check_dataset:
            dataset_id = str(check_dataset.dataset_id)
        else:
            dataset = Dataset(job_id=str(job_id), region=region, year=year, crop=crop, feature_geom=geom_wkt,
                              data_product_unique_id=data_product_unique_id)
            df_session.add(dataset)
            df_session.commit()
            dataset_id = str(dataset.dataset_id)

    print(f"Total number of grids to process: {number_of_grids}")
    print(f"Total number of grids excluded: {number_of_grids-len(global_reference_grids)}")
    print(f"Total number of grids will be processed: {len(global_reference_grids)}")

    if provider_obj:
        if provider_obj.limit and provider_obj.limit != 0:
            for grids in range(0,len(global_reference_grids),provider_obj.limit):
                # Pause Jobs in Grids-Data-Build Dag
                pause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
                pause_dag.is_paused = True
                session.commit()
                exe = ThreadPoolExecutor(max_workers=int(MAX_WORKERS))
                exe.map(functools.partial(trigger_task,name=name,provider=provider,provider_params=provider_params,
                                          job_id=job_id,validate=validate,context_code=context_code,
                                          measure_dimension=measure_dimension,temporal_dimension=temporal_dimension,
                                          product_dimension=product_dimension,spatial_dimension=spatial_dimension,
                                          dataset_id=dataset_id,irrigated_dataset_id=irrigated_dataset_id,
                                          non_irrigated_dataset_id=non_irrigated_dataset_id, crop=crop,
                                          ),
                        enumerate(global_reference_grids[grids:grids + provider_obj.limit]))
                exe.shutdown(wait=True)
                # Unpause the Grids-Data-Build Jobs which starts processing all the queued jobs
                unpause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
                unpause_dag.is_paused = False
                session.commit()
                sleep_time = timedelta(seconds=int(wait_time))
                time.sleep(int(sleep_time.total_seconds()))
        else:
            exe = ThreadPoolExecutor(max_workers=int(MAX_WORKERS))
            exe.map(functools.partial(trigger_task, name=name, provider=provider, provider_params=provider_params,
                                      job_id=job_id, validate=validate, context_code=context_code,
                                      measure_dimension=measure_dimension, temporal_dimension=temporal_dimension,
                                      product_dimension=product_dimension, spatial_dimension=spatial_dimension,
                                      dataset_id=dataset_id, irrigated_dataset_id=irrigated_dataset_id,
                                      non_irrigated_dataset_id=non_irrigated_dataset_id, crop=crop),
                    enumerate(global_reference_grids))
            exe.shutdown(wait=True)

    print("Done...")

    provider_id = df_session.query(Providers).filter(Providers.name == provider.lower()).first()
    if measure_dimension:
        provider_variables = df_session.query(ProviderVariables).\
            filter(ProviderVariables.provider_id == provider_id.provider_id,
                   ProviderVariables.code.in_(measure_dimension)).all()
    else:
        provider_variables = df_session.query(ProviderVariables).\
            filter(ProviderVariables.provider_id == provider_id.provider_id).all()

    query = PROVIDERSCHEMALOOKUP[provider.lower()](provider_variables,temporal_dimension,spatial_dimension,product_dimension, data_product_unique_id,
                 job_id, dataset_id).create_query_template()

    print("-------------------- Query -------------------")
    print(query)
    print("----------------------------------------------")
    if irrigated_dataset_id:
        print(f"""
                    **Note: Above query is to fetch Data Product where Irrigation data was Null.
                    Please replace dataset_id as {irrigated_dataset_id} to get Irrigated Data Product.
                    Please replace dataset_id as {non_irrigated_dataset_id} to get Non-Irrigated Data Product.
                """)

    #Unpause the Grids-Data-Build Jobs which starts processing all the queued jobs
    unpause_dag = session.query(DagModel).filter(DagModel.dag_id == "Grids-Data-Build").first()
    unpause_dag.is_paused = False
    session.commit()

    if trigger_fme:
        execution_date = timezone.utcnow()
        if irrigated_dataset_id:
            irrigated_fme_trigger = trigger_dag(dag_id='Trigger-FME', execution_date=execution_date,
                                                replace_microseconds=False,
                                                conf={'primary_job_id': job_id, 'dataset_id': irrigated_dataset_id,
                                                      'global_reference_grids': global_reference_grids,'fme_params':fme_params,
                                                      'data_product_unique_id':data_product_unique_id,'provider':provider.lower()})
            non_irrigated_fme_trigger = trigger_dag(dag_id='Trigger-FME', execution_date=execution_date,
                                                    replace_microseconds=False,
                                                    conf={'primary_job_id': job_id, 'dataset_id': non_irrigated_dataset_id,
                                                          'global_reference_grids': global_reference_grids,'fme_params':fme_params,
                                                          'data_product_unique_id':data_product_unique_id,'provider':provider.lower()})
        else:
            trigger_dag(dag_id='Trigger-FME', execution_date=execution_date,replace_microseconds=False,
                        conf={'primary_job_id': job_id, 'dataset_id': dataset_id, 'fme_params':fme_params,
                              'global_reference_grids': global_reference_grids,'data_product_unique_id':data_product_unique_id,
                              'provider':provider.lower()})
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
        'Fetch-Reference-Grids',
        default_args=default_args,
        description='A Fetch-Reference-Grids DAG',
        schedule=None,
        start_date=days_ago(1),
        tags=['Fetch-Reference-Grids'],
        params=sample_template,
) as dag:
    t1 = BashOperator(
        task_id='on_start',
        bash_command='date',
    )

    t2 = PythonOperator(
        task_id='get_global_reference_grids',
        depends_on_past=False,
        python_callable=get_global_reference_grids,
        retries=0,
    )

    t1 >> t2
