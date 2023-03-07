import io
import json
import time
import uuid
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import settings
import pandas as pd
import os
import boto3
import botocore
from services.db_service import Session, engine
from models.datafactory import FileProcessStatus, Providers, MarketFacts, Definition, \
    ProviderVariables, StandardVariables, UnitOfMeasurement, Language
import geopandas
from airflow.configuration import conf
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import xmltodict

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))
AWS_ACCCESS_KEY_ID = str(os.getenv("AWS_ACCCESS_KEY_ID", None))
AWS_SECRET_ACCESS_KEY = str(os.getenv("AWS_SECRET_ACCESS_KEY", None))
BUCKET = str(os.getenv("BUCKET", None))
MANIFEST_KEY = str(os.getenv("MANIFEST_KEY", None))

session = settings.Session()
df_session = Session()

#DB table names mapped to Class names
db_tables={
    'definition':Definition,
    'market_facts':MarketFacts,
    'data_provider':Providers,
    'provider_variables':ProviderVariables,
    'standard_variables':StandardVariables,
    'unit_of_measurement':UnitOfMeasurement,
    'language':Language,
    'grid_geom':'grid_geom'
}

class ProcessFiles:
    '''
    connecting to s3 bucket and process files
    Parameters:
    file_path: file path in S3 bucket
    '''
    def __init__(self,file_path):
        self.s3 = boto3.client(
            service_name='s3',
            region_name=None,
            aws_access_key_id=AWS_ACCCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        try:
            self.obj = self.s3.get_object(Bucket=BUCKET, Key=file_path)
        except botocore.exceptions.ClientError as error:
            raise error

    #process shp file to update db
    def shapefile(self, file_name, target_table, date,job_id):
        """ Push global reference grids data in database
        Parameters:
        file_name: Name of the file to process
        target_table: table name to which data to be added or updated
        date: Date the file created
        job_id: Job id created by airflow
        """
        try:
            shape_file = geopandas.read_file(self.obj["Body"])
            # Drop rows containing CROPLAND_V = 0
            _shape_file = shape_file[(shape_file[['CROPLAND_V']] != 0).all(axis=1)]
            #rename columns to ingest data to db
            shape_file = _shape_file.rename(
                columns={
                    'ROW_ID': 'grid_id',
                    'AREA_HA_V': 'hactares',
                    'LATITUDE': 'latitude',
                    'LONGITUDE': 'longitude',
                    'COUNTRY': 'country',
                    'CODE_ISO2': 'country_code',
                    'CODE_ISO3': 'code_iso3',
                    'REGION': 'region',
                    'place_id':'place_id',
                    'CROPLAND_V': 'cropland_v',
                    'LANDMASK_V': 'landmask_v',
                },
                inplace=False)
            shape_file.to_postgis('grid_geom', con=engine, if_exists='append', schema='datafactory',
                index=False, chunksize=4000)
            #update in local table that the file is processed
            update_file_process = FileProcessStatus(file_name=file_name,target_table=target_table,status="success",
                                                    job_id=job_id,file_created=date)
            df_session.add(update_file_process)
            df_session.commit()
        except Exception as e:
            print(f'Task failed with exception as {e}')
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table, status="failed",
                                                    job_id=job_id, file_created=date, note=e)
            df_session.add(update_file_process)
            df_session.commit()
        return "OK"

    #process json type file and update data in DB
    def jsonfiles(self,file_name, target_table, date, job_id):
        """
        json type files data ingest to DB
        Parameters:
        file_name: Name of the file to process
        target_table: table name to which data to be added or updated
        date: Date the file created
        job_id: Job id created by airflow
        """
        try:
            db_columns = db_tables[target_table].__table__.columns.keys()
            json_obj = json.load(self.obj['Body'])
            json_keys = json_obj[0].keys()
            if set(json_keys).issubset((db_columns)):
                bulk_insert_values = insert(db_tables[target_table]).values(json_obj).on_conflict_do_nothing()
            else:
                raise Exception(f'Provided Json Keys not matching with db schema of table {target_table}')
            df_session.execute(bulk_insert_values)
            df_session.commit()
            #update in local table with processed file details
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table,
                                                    status="success",job_id=job_id, file_created=date)
            df_session.add(update_file_process)
            df_session.commit()
        except Exception as e:
            print(f'Task failed with exception as {e}')
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table, status="failed",
                                                    job_id=job_id, file_created=date, note=e)
            df_session.add(update_file_process)
            df_session.commit()
        return "OK"

    #process excel file type to update DB
    def excelfiles(self, file_name, target_table, date, job_id):
        """
        process excel type files
        Parameters:
        file_name: Name of the file to process
        target_table: table name to which data to be added or updated
        date: Date the file created
        job_id: Job id created by airflow
        """
        try:
            xlData = pd.read_excel(io.BytesIO(self.obj['Body'].read()), skiprows=0,engine='openpyxl')
            excel_columns = list(xlData)
            db_columns = db_tables[target_table].__table__.columns.keys()
            if set(list(excel_columns)).issubset((db_columns)):
                if 'product_dimension' in xlData.columns:
                    xlData['product_dimension'] = xlData['product_dimension'].apply(json.loads)
                uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{file_name}_{job_id}"))
                xlData['file_process_job_id'] = str(uuid_gen)
                result = xlData.to_json(orient="records")
                parsed = json.loads(result)
                bulk_insert_values = insert(db_tables[target_table]).values(parsed). \
                    on_conflict_do_nothing(index_elements=['market_facts_id'])
            else:
                raise Exception(f'Provided target table {target_table} is incorrect. Please check')

            df_session.execute(bulk_insert_values)
            df_session.commit()
            #update in local table with processed file details
            update_file_process = FileProcessStatus(file_process_status_id=uuid_gen,file_name=file_name, target_table=target_table,
                                                    status="success",job_id=job_id, file_created=date)
            df_session.add(update_file_process)
            df_session.commit()
        except Exception as e:
            df_session.rollback()
            print(f'Task failed with exception as {e}')
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table, status="failed",
                                                    job_id=job_id, file_created=date, note=e)
            df_session.add(update_file_process)
            df_session.commit()
        return "OK"

    # process csv file type to update DB
    def csvfiles(self, file_name, target_table, date, job_id):
        """
        process csv type files
        Parameters:
        file_name: Name of the file to process
        target_table: table name to which data to be added or updated
        date: Date the file created
        job_id: Job id created by airflow
        """
        time.sleep(10)
        try:
            csvData = pd.read_csv(self.obj['Body'], skiprows=0)
            csv_columns = list(csvData)
            db_columns = db_tables[target_table].__table__.columns.keys()
            if set(list(csv_columns)).issubset((db_columns)):
                result = csvData.to_json(orient="records")
                parsed = json.loads(result)
                bulk_insert_values = insert(db_tables[target_table]).values(parsed). \
                    on_conflict_do_nothing(index_elements=['provider_variable_id'])
            else:
                raise Exception(f'Provided target table {target_table} is incorrect. Please check')

            df_session.execute(bulk_insert_values)
            df_session.commit()
            # update in local table with processed file details
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table,
                                                    status="success", job_id=job_id, file_created=date)
            df_session.add(update_file_process)
            df_session.commit()
        except Exception as e:
            df_session.rollback()
            print(f'Task failed with exception as {e}')
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table, status="failed",
                                                    job_id=job_id, file_created=date, note=e)
            df_session.add(update_file_process)
            df_session.commit()
        return "OK"

    # process xml file type to update DB
    def xmlfile(self, file_name, target_table, date, job_id):
        """
        convert xml to json and update data to UOM db tables
        Parameters:
        file_name: Name of the file to process
        target_table: table name to which data to be added or updated
        date: Date the file created
        job_id: Job id created by airflow
        """
        try:
            data_dict = xmltodict.parse(self.obj['Body'].read().decode('utf-8'))
            json_data = json.dumps(data_dict)
            obj = json.loads((json_data))
            uom = {'uom_name': ""}

            for unitsystems in obj['UnitSystem']['UnitDimensions']['UnitDimension']:
                for unitkey, unitvalue in unitsystems.items():
                    if unitkey == "UnitDimensionRepresentation":
                        for unitsysKey, unitsysvalue in unitvalue.items():
                            if unitsysKey == "@domainID":
                                uom.update({'uom_name': unitsysvalue})
                            if unitsysKey == "UnitOfMeasure":
                                if isinstance(unitsysvalue, list):
                                    for unitmeasurement in unitsysvalue:
                                        process_uom(uom['uom_name'], unitmeasurement)
                                elif isinstance(unitsysvalue, dict):
                                    process_uom(uom['uom_name'], unitsysvalue)

            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table,
                                                    status="success", job_id=job_id, file_created=date)
            df_session.add(update_file_process)
            df_session.commit()
        except Exception as e:
            print(f'Task failed with exception as {e}')
            update_file_process = FileProcessStatus(file_name=file_name, target_table=target_table, status="failed",
                                                    job_id=job_id, file_created=date, note=e)
            df_session.add(update_file_process)
            df_session.commit()
        return "OK"


def process_uom(name, unitmeasurement):
    add_uom_obj = UOMInsert().add_uom_obj(name, unitmeasurement)
    for umkeys, umvalues in unitmeasurement.items():
        if umkeys == "Name":
            if isinstance(umvalues, list):
                for umvalue in umvalues:
                    UOMInsert().add_language(umvalue, add_uom_obj)
            elif isinstance(umvalues, dict):
                UOMInsert().add_language(umvalues, add_uom_obj)


class UOMInsert:
    """
    update UOM data to db
    """
    def add_uom_obj(self, name, data_obj):
        code = data_obj['@domainID']
        scale = data_obj['@scale']
        offset = data_obj['@baseOffset']
        if scale == "1" and offset == "0":
            standard = True
        else:
            standard = False
        uom_exist = df_session.query(UnitOfMeasurement).filter(UnitOfMeasurement.name == name,UnitOfMeasurement.code==code,
                                                            UnitOfMeasurement.scale == scale,
                                                            UnitOfMeasurement.offset == offset).first()
        if not uom_exist:
            add_uom = UnitOfMeasurement(name=name, code=code, scale=scale,
                                        offset=offset, uom_is_standard=standard)
            df_session.add(add_uom)
            df_session.commit()
            return add_uom.unit_of_measurement_id
        else:
            return uom_exist.unit_of_measurement_id

    def add_language(self, data_obj, uom_id):
        lcode = data_obj['@locale']
        llabel = data_obj['@label']
        lname = data_obj['@plural']
        lang_exist = df_session.query(Language).filter(Language.name==lname,Language.language_code == lcode,
                                                    Language.label == llabel,
                                                    Language.unit_of_measurement_id == uom_id).first()
        if not lang_exist:
            add_lan = Language(name=lname, language_code=lcode, label=llabel,
                               unit_of_measurement_id=uom_id)
            df_session.add(add_lan)
            df_session.commit()


def process_ref_data_file(**kwargs):
    '''
    function to check file type and process reference files.
    '''
    _data = kwargs['params']
    file_name = _data.get('file_name', None)
    file_path = _data.get('file_path', None)
    target_table = _data.get('target_table', None)
    file_type = _data.get('file_type', None)
    date = _data.get('date', None)
    job_id = kwargs[
        'dag_run'].run_id if kwargs['dag_run'] else 'test_dag'
    if file_type == "zip":
        ProcessFiles(file_path).shapefile(file_name, target_table, date, job_id)
    elif file_type == "json":
        ProcessFiles(file_path).jsonfiles(file_name, target_table, date, job_id)
    elif file_type == "xlsx":
        ProcessFiles(file_path).excelfiles(file_name, target_table, date, job_id)
    elif file_type == "csv":
        ProcessFiles(file_path).csvfiles(file_name, target_table, date, job_id)
    elif file_type == "xml":
        ProcessFiles(file_path).xmlfile(file_name, target_table, date, job_id)


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
        'Process-Reference-Data-File',
        default_args=default_args,
        description='A DAG to process reference data files',
        schedule=None,
        start_date=days_ago(1),
        tags=['Process-Reference-Data-File'],
) as dag:
    process_ref_file = PythonOperator(
        task_id='process_ref_file',
        depends_on_past=False,
        python_callable=process_ref_data_file,
        retries=0,
    )
    process_ref_file
