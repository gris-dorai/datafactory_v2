from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
# Operators; we need this to operate!
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import settings
import pandas as pd
import os
import boto3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from services.db_service import Session
from dotenv import load_dotenv


file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))
AWS_ACCCESS_KEY_ID = str(os.getenv("AWS_ACCCESS_KEY_ID", None))
AWS_SECRET_ACCESS_KEY = str(os.getenv("AWS_SECRET_ACCESS_KEY", None))
ENV = str(os.getenv("ENV", None))
BUCKET = str(os.getenv("BUCKET", None))
MANIFEST_KEY = str(os.getenv("MANIFEST_KEY", None))
REFERENCE_FILE_UPDATE_DIR = str(os.getenv("REFERENCE_FILE_UPDATE_DIR", None))
REFERENCE_FILE_DIR = str(os.getenv("REFERENCE_FILE_DIR", None))

session = settings.Session()
df_session = Session()


def register_ref_file(**kwargs):

    _data = kwargs['params']
    file_name = _data.get("file_name", None)
    target_table = _data.get("target_table", None)
    if file_name and target_table:
        file_extension = os.path.splitext(file_name)[1][1:]

        target_table_validation = {"grid_geom": "zip","definition":"json","standard_variables":"json",
                                   "data_provider":"json","market_facts":"xlsx","unit_of_measurement":"xml",
                                   "provider_variables":"csv"}
        if target_table_validation.get(target_table,None) != file_extension:
            raise Exception("Updating reference data to target table is not supported for the given file extension")

        s3 = boto3.client(
            service_name='s3',
            region_name=None,
            aws_access_key_id=AWS_ACCCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        src_key = '/'.join([REFERENCE_FILE_UPDATE_DIR, file_name])
        dest_key = '/'.join([REFERENCE_FILE_DIR, file_name])

        try:
            s3.head_object(Bucket=BUCKET, Key=src_key)
        except s3.exceptions.NoSuchKey:
            raise Exception("The specified object does not exist in S3.")

        manifest_file_obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        csvData = pd.read_csv(manifest_file_obj['Body'])
        if csvData["file_path"].isin([dest_key]).sum() > 0:
            raise Exception('File already exists in manifest file')

        s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': src_key}, Key=dest_key)
        new_row = {'file_name': file_name, 'file_path': dest_key,'target_table':target_table,'file_type':file_extension,
                   'required':True,'environment':ENV,'date': str(datetime.now().strftime('%d_%m_%Y'))}
        df = csvData.append(new_row, ignore_index=True)

        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=df.to_csv(index=False).encode())

        s3.delete_object(Bucket=BUCKET,Key=src_key)
        current_datetime = (datetime.timestamp(datetime.now()))
        TriggerDagRunOperator(
            task_id=f'process_manifest_file_{current_datetime}',
            trigger_dag_id="Process-Manifest-File",
            wait_for_completion=False,
            conf={'file_path': dest_key},
            dag=dag
        ).execute(context=kwargs)

    else:
        raise Exception("Please provide the file name and target table to register and update reference data")


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
        'Register-Update-Reference-File',
        default_args=default_args,
        description='A DAG to register and update reference file',
        schedule=None,
        start_date=days_ago(1),
        tags=['Register-Update-Reference-File'],
        params={
            "file_name":"",
            "target_table":"",
            "environment":"development"
            }
) as dag:
    register_ref_file = PythonOperator(
        task_id='register_ref_file',
        depends_on_past=False,
        python_callable=register_ref_file,
        retries=0,
    )

    register_ref_file