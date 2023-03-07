import io
import json
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import settings
import pandas as pd
import os
import boto3
import botocore
from dotenv import load_dotenv
from sqlalchemy import Table, create_engine, delete
from airflow.models.base import Base
from models.datafactory import FileProcessStatus, Providers, MarketFacts, Definition, \
    ProviderVariables, StandardVariables, UnitOfMeasurement, Language
from services.db_service import Session, engine

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))
AWS_ACCCESS_KEY_ID = str(os.getenv("AWS_ACCCESS_KEY_ID", None))
AWS_SECRET_ACCESS_KEY = str(os.getenv("AWS_SECRET_ACCESS_KEY", None))
ENV = str(os.getenv("ENV", None))
BUCKET = str(os.getenv("BUCKET", None))
MANIFEST_KEY = str(os.getenv("MANIFEST_KEY", None))

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


def delete_from_db(json_obj,target_table):
    """
    delete data from the target table
    Parameters:
    json_obj: json object which is fetched and converted from dataframe to be deleted
    target_table: table name where the objects to be found and deleted
    """
    for file_data in json_obj:
        query = df_session.query(target_table)
        for attr, value in file_data.items():
            query = query.filter(getattr(target_table, attr) == value)
        query.delete()
    df_session.commit()


def delete_ref_data(**kwargs):
    """
    Delete reference data from DB and from S3
    """
    _data = kwargs['params']
    file_path = _data.get("file_path", None)
    manifest_file_url = _data.get("manifest_file_url", MANIFEST_KEY)

    s3 = boto3.client(
        service_name='s3',
        region_name=None,
        aws_access_key_id=AWS_ACCCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=manifest_file_url)
    except botocore.exceptions.ClientError as error:
        raise error

    #read manifest file to check if the file exist in S3 to delete
    csvData = pd.read_csv(obj['Body'])
    if file_path:
        #get row matching with file path in manifest file
        get_row = csvData.loc[(csvData['file_path'] == file_path)]
        target_table = db_tables[get_row['target_table'].values[0]]
        file_name = get_row['file_name'].values[0]
        file_type = get_row['file_type'].values[0]

        #to check if the file process is success before deleting the records from DB
        file_processed = df_session.query(FileProcessStatus).filter(FileProcessStatus.file_name == file_name,
                                                                 FileProcessStatus.status == "success")
        try:
            get_obj = s3.get_object(Bucket=BUCKET, Key=file_path)
        except botocore.exceptions.ClientError as error:
            raise error

        #delete all the records from DB matching the file records
        if file_processed.first() and file_type == "xlsx":
            xlData = pd.read_excel(io.BytesIO(get_obj['Body'].read()), skiprows=0, engine='openpyxl')
            to_json = xlData.to_json(orient="records")
            json_obj = json.loads(to_json)
            delete_from_db(json_obj,target_table)
        elif file_processed.first() and file_type == "json":
            json_obj = json.load(get_obj['Body'])
            delete_from_db(json_obj,target_table)
        elif file_processed.first() and file_type == "zip":
            get_table = Table(target_table, Base.metadata,autoload=True, autoload_with=engine,
                              schema='datafactory')
            delete_data = delete(get_table)
            df_session.execute(delete_data)
            df_session.commit()

        #delete a row from manifest file
        get_row_index = csvData.index[(csvData['file_path'] == file_path)]
        get_index = get_row_index.tolist()
        update_rows = csvData.drop(labels=get_index, axis=0)
        csv_buffer = StringIO()
        update_rows.to_csv(csv_buffer, header=True, index=False)
        try:
            s3.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET, Key=manifest_file_url)
        except botocore.exceptions.ClientError as error:
            raise error

        #delete file from S3 bucket
        try:
            s3.delete_object(Bucket=BUCKET, Key=file_path)
        except botocore.exceptions.ClientError as error:
            raise error

        #delete a record from FileProcessStatus table
        file_processed.delete()
        df_session.commit()
        return "OK"


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
        'Delete-Reference-Data',
        default_args=default_args,
        description='A DAG to delete reference data',
        schedule=None,
        start_date=days_ago(1),
        tags=['Delete-Reference-Data'],
        params={
            "file_path": "",
            "manifest_file_url":""
        }
) as dag:
    delete_reference_data = PythonOperator(
        task_id='delete_reference_data',
        depends_on_past=False,
        python_callable=delete_ref_data,
        retries=0,
    )

    delete_reference_data