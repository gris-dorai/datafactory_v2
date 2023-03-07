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
from models.datafactory import FileProcessStatus
from dotenv import load_dotenv
import botocore

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))
AWS_ACCCESS_KEY_ID = str(os.getenv("AWS_ACCCESS_KEY_ID", None))
AWS_SECRET_ACCESS_KEY = str(os.getenv("AWS_SECRET_ACCESS_KEY", None))
ENV = str(os.getenv("ENV", None))
BUCKET = str(os.getenv("BUCKET", None))
MANIFEST_KEY = str(os.getenv("MANIFEST_KEY", None))

session = settings.Session()
df_session = Session()

def process_manifest_file(**kwargs):
    '''
    Process manifest file and trigger dag to process each file and update db
    '''
    #Get config parameters to process only particular file
    _data = kwargs['params']
    file_path = _data.get("file_path", None)
    manifest_file_url = _data.get("manifest_file_url", MANIFEST_KEY)
    current_datetime = (datetime.timestamp(datetime.now()))
    #connect to s3 bucket
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
    csvData = pd.read_csv(obj['Body'])
    #if parameters is provided in config and is not none. process particular file.
    if file_path:
        #from manifest file get details of provided file_path in S3
        file_details = csvData.loc[(csvData['file_path'] == file_path)]
        #get all the required parameters value to trigger dag
        obj_name = file_details['file_name'].values[0]
        obj_path = file_details['file_path'].values[0]
        obj_type = file_details['file_type'].values[0]
        obj_target_table = file_details['target_table'].values[0]
        obj_required = file_details['required'].values[0]
        obj_environment = file_details['environment'].values[0]
        obj_date = file_details['date'].values[0]
        #check if file already processed
        file_processed = df_session.query(FileProcessStatus).filter(FileProcessStatus.file_name == obj_name,
                                                                 FileProcessStatus.status == "success").first()
        if not file_processed and obj_required and obj_environment in [ENV, "all"]:
            #trigger process refernce file dag with config details
            TriggerDagRunOperator(
                task_id=f'process_ref_file_{obj_name}_{current_datetime}',
                trigger_dag_id="Process-Reference-Data-File",
                wait_for_completion=False,
                conf={'file_name': obj_name, 'file_path': obj_path, 'target_table': obj_target_table,
                      'file_type': obj_type,'date': obj_date},
                dag=dag
            ).execute(context=kwargs)
    else:
        #check each row in manifest file and process files
        for i, row in csvData.iterrows():
            row.fillna('', inplace=True)
            file_processed = df_session.query(FileProcessStatus).filter(FileProcessStatus.file_name == row['file_name'],
                                                                     FileProcessStatus.status == "success").first()
            if not file_processed and row['required'] and row['environment'] in [ENV, "all"]:
                #trigger dag to process files
                TriggerDagRunOperator(
                    task_id=f'process_ref_file_{current_datetime}_{i}',
                    trigger_dag_id="Process-Reference-Data-File",
                    wait_for_completion=False,
                    conf={'file_name':row['file_name'],'file_path':row['file_path'],'target_table':row['target_table'],
                          'file_type':row['file_type'],'date':row['date']},
                    dag=dag
                ).execute(context=kwargs)
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
        'Process-Manifest-File',
        default_args=default_args,
        description='A DAG to process manifest file',
        schedule=None,
        start_date=days_ago(1),
        tags=['Process-Manifest-File'],
        params={
            "file_path":""
            }
) as dag:
    process_file = PythonOperator(
        task_id='process_file',
        depends_on_past=False,
        python_callable=process_manifest_file,
        retries=0,
    )

    process_file