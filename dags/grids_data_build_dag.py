import os
import json
from dotenv import load_dotenv
from datetime import timedelta
from shapely.geometry import Point
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
# custome modes for storing data
from services.db_service import Session
from services.production_service import FieldProduction
from services.provider_service import PROVIDERLOOKUP
from models.datafactory import Providers, Validations, Statistics, ProviderVariables
from airflow.models import Variable
from shapely import wkt
from shapely.geometry import shape, mapping
from shapely.geometry.multipolygon import MultiPolygon
from airflow import settings
from services import variable_service

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))

MAX_ACTIVE_DAGS = int(os.getenv("MAX_ACTIVE_DAGS", None))
MAX_ACTIVE_TASKS = int(os.getenv("MAX_ACTIVE_TASKS", None))

session = settings.Session()
df_session = Session()

max_active_tasks = Variable.get('max_active_tasks', 20)


def process_grid(**kwargs):
    """
    Dag to get grids from GLOBAL_REFRENCE_API passed params
    """
    # get the configuration for job
    _data = kwargs['params']
    provider = _data.get('provider', None)
    provider_params = _data.get('provider_params', None)
    geometry = _data.get('geometry', None)
    job_id = _data.get('job_id', None)
    dataset_list = _data.get('dataset_list', None)
    validate = _data.get('validate', None)
    grid_id = geometry.get('grid_id', None)
    context_code = _data.get('context_code', None)
    process_job_id = kwargs['dag_run'].run_id if kwargs['dag_run'] else 'test_grids_data_build_dag'
    measure_dimension = _data.get('measure_dimension', None)
    temporal_dimension = _data.get('temporal_dimension', None)
    product_dimension = _data.get('product_dimension', None)
    spatial_dimension = _data.get('spatial_dimension', None)

    # check and get job_id and its state if exist
    get_task_state = Variable.get(job_id, None)

    # if job_id key value is provided as fail then fail the Grids-Data-Build task
    if get_task_state and get_task_state == "fail":
        Variable.delete(job_id)
        raise AirflowException("Adding Task to failed state")

    provider_api_response = PROVIDERLOOKUP[provider.lower()](provider).\
        get_api_response(process_job_id,provider_params,temporal_dimension, product_dimension,
                         spatial_dimension, geometry, dataset_list, validate)

    datasetgrid_objects = {'generic': DatasetGridGeneric, 'irrigated': DatasetGridIrrigated,
                           'non-irrigated': DatasetGridNonIrrigated}

    if provider_api_response:
        # create a list dataset grid objects
        for features_collection in provider_api_response:
            dataset_grid_obj_list = []
            dataset_filtered_list = features_collection.get('properties',None).get('dataset_list',None)
            for dataset_params in dataset_filtered_list:
                if dataset_params['dataset_id']:
                    dataset_grid_obj = datasetgrid_objects[dataset_params['type']] \
                        (dataset_id=dataset_params['dataset_id'], geometry=geometry, provider_name=provider,
                         process_job_id=process_job_id, job_id=job_id, grid_id=grid_id, context_code=context_code,
                         summary_validate=validate, api_response=features_collection, measure_dimension=measure_dimension,
                         temporal_dimension=features_collection.get('properties',None).get("temporal_dimension",None),
                         spatial_dimension=features_collection.get('properties',None).get("spatial_dimension",None),
                         product_dimension=features_collection.get('properties',None).get("product_dimension",None))
                    dataset_grid_obj_list.append(dataset_grid_obj)
            # validate field objects and load variables
            for field_obj in features_collection['features']:
                for dataset_obj in dataset_grid_obj_list:
                    if dataset_obj.validate(field_obj):
                        dataset_obj.get_field_obj(field_obj)
            # calculate variables for each dataset object
            for dataset_obj in dataset_grid_obj_list:
                dataset_obj.calculate_variables()
    return "OK"


class DatasetGridGeneric:
    """
    Dataset Grid Generic Method
    Parameters:
    dataset_id: Dataset object which is created initially
    geometry: 10*10 KM grid from global reference mart
    provider_name: Name of the provider
    process_job_id: Secondary Dag Job ID
    job_id: Primary Dag Job ID
    grid_id: Grid ID for which the data is processing
    context_code: Code used to validate value
    summary_validate: If the statistics summary should be validated or not
    api_response: Response returned by the API
    measure_dimension: Measure dimension data provided in the config which Contains list of variables to be processed
    temporal_dimension: Temporal dimension data provided in the config which contains year, season, growth stage values
    spatial_dimension: Spatial dimension data provided in the config which contains Region, geometry, country values
    product_dimension: Product dimension data provided in the config which contains crop, irrigation values
    """
    def __init__(self, dataset_id, geometry, provider_name, process_job_id, job_id, grid_id, context_code,
                 summary_validate,api_response, measure_dimension, temporal_dimension, spatial_dimension,
                 product_dimension):

        self.context_code = context_code
        self.geometry = geometry
        self.total_grid_area = geometry.get('hactares', None)
        provider_id = df_session.query(Providers).filter(Providers.name == provider_name.lower()).first()
        if measure_dimension:
            providervariable_ids = df_session.query(ProviderVariables). \
                filter(ProviderVariables.provider_id == provider_id.provider_id,
                       ProviderVariables.code.in_(measure_dimension)).all()
        else:
            providervariable_ids = df_session.query(ProviderVariables). \
                filter(ProviderVariables.provider_id == provider_id.provider_id).all()

        self.variables = [variable_service.VariableCalculator(providervariable_id.provider_variable_id) for
                          providervariable_id in providervariable_ids]
        self.process_job_id = process_job_id
        self.grid_id = grid_id
        self.job_id = job_id
        self.dataset_id = dataset_id
        self.summary_validate = summary_validate
        self.api_response = api_response
        self.summary_object = []
        self.temporal_dimension = temporal_dimension
        self.spatial_dimension = spatial_dimension
        self.product_dimension = product_dimension

    def get_field_obj(self, field_obj):
        """
        Fetch field object and load variables
        """
        self.field_obj = field_obj
        self.load_variables()

    def load_variables(self):
        """
        load value for each variable
        """
        for variable in self.variables:
            variable.load_value(self.field_obj, self.context_code)

    def calculate_variables(self):
        """
        calculate value for provider variables
        """
        for variable in self.variables:
            self.data = {}
            self.prov_var_id, self.value = variable.calculate_variable(self.total_grid_area)
            self.data['variable'] = str(self.prov_var_id)
            if self.value != None:
                if isinstance(self.value, str):
                    self.data['value'] = self.value
                else:
                    self.data['value'] = round(self.value, 2)
            else:
                self.data['value'] = None
            self.summary_object.append(self.data)

        print("==================================================")
        print(f"summary : {self.summary_object}")
        print("==================================================")
        self.save_summary()

    def save_summary(self):
        """
        Save summary statistics to Statistics Table
        """
        temporal_dimension = self.temporal_dimension
        # spatial_dimension = {"COUNTRY":self.geometry['country_code'],"REGION":self.geometry['region']}
        spatial_dimension = self.spatial_dimension
        product_dimension = self.product_dimension
        add_statistics = Statistics(job_id=self.job_id, grid_id=self.grid_id, dataset_id=self.dataset_id,
                                    summary=self.summary_object, grid_process_id=self.process_job_id,
                                    temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                                    product_dimension=product_dimension,status="success")
        df_session.add(add_statistics)
        df_session.commit()
        statistics_obj = add_statistics
        if self.summary_validate:
            self.perform_validation(self.job_id, statistics_obj.statistics_id, self.grid_id, self.api_response,self.process_job_id)

    def perform_validation(self, job_id, statistics_id, grid_id, api_response, grid_process_id):
        """ Save Validation data """
        raster_dir = str(os.getenv("RASTER_DIR", None))
        file_path = str(os.getenv("FILE_PATH", None))
        path = os.path.join(file_path, raster_dir)
        file_loc = f'{path}/{grid_process_id}.tiff'
        if os.path.exists(file_loc):
            file = file_loc
            efs_file = file_loc.replace("opt", "efs")
        else:
            file = None
            efs_file = None
        try:
            validations = Validations()
            validations.job_id = job_id
            validations.grid_process_id = grid_process_id
            validations.file_loc = file
            validations.file_loc_efs = efs_file
            validations.statistics_id = statistics_id
            validations.grid_id = grid_id
            validations.fields = api_response
            validations.insert()
        except Exception as ex:
            raise AirflowException(f"Failed to Store data on DB : {ex}")

    def validate(self, field_obj):
        """
        validate method to check if irrigated or non-irrigated
        """
        if not field_obj['geometry']:
            return True
        get_wkt = shape(json.loads((self.geometry['geom'])))
        geom_wkt = get_wkt.wkt
        _geom = wkt.loads(geom_wkt)
        if _geom.geom_type == 'Polygon':
            geom_wkt = (MultiPolygon([_geom])).wkt
        g1 = wkt.loads(geom_wkt)
        g2 = mapping(g1)
        grid_geom = shape(json.loads(json.dumps(g2)))
        data_centroid = Point((shape(field_obj['geometry'])).centroid)
        contains = grid_geom.contains(data_centroid)
        touches = grid_geom.touches(data_centroid)
        if contains or touches:
            return True


class DatasetGridIrrigated(DatasetGridGeneric):
    """
    Dataset Grid Irrigated Method
    """
    def validate(self, field_obj):
        """
        To check if field object is irrigated
        """
        yeild_production, quotient = FieldProduction(self.geometry, self.context_code).get_yield_production(field_obj)
        # compare the yield_estimate, also with isoperimetric quotient
        if yeild_production and quotient > 0.8:
            return True
        else:
            return False


class DatasetGridNonIrrigated(DatasetGridIrrigated):
    """
    Dataset Grid Non-Irrigated Method
    """
    def validate(self, field_obj):
        """
        To check if field object is Non-Irrigated
        """
        yeild_production, quotient = FieldProduction(self.geometry, self.context_code).get_yield_production(field_obj)
        # compare the yield_estimate, also with isoperimetric quotient
        if not yeild_production or not quotient > 0.8:
            return True
        else:
            return False


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
        'Grids-Data-Build',
        default_args=default_args,
        description='A Grids-Data-Build DAG',
        schedule=None,
        start_date=days_ago(1),
        tags=['Grids-Data-Build'],
        max_active_runs=int(max_active_tasks),
        concurrency=int(max_active_tasks),
        params={
            "name": "",
            "crop": "",
            "year": "",
            "geometry": {},
            "job_id": "job_id",
            "dataset_id": "dataset_id",
            "validate": False
        }
) as dag:
    t1 = PythonOperator(
        task_id='process_grid',
        depends_on_past=False,
        python_callable=process_grid,
        retries=0,
    )

    t1
