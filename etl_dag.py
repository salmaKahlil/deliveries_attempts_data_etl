from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_phase import DataExtractor
from transform_phase import DataTransformer
from load_phase import DataLoader
from airflow.utils.dates import days_ago

from config import (
    REDSHIFT_PARAMS,
    MONGO_CONNECTION_STRING,
    MONGO_DATABASE,
    MONGO_COLLECTION,
    S3_BUCKET_NAME,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    ETL_JOB_NAME,
    MSG_TEXT,
    logger,
    EGYPT_TZ,
    COLUMNS_TO_SELECT,
    DATA_TYPES,
    REDSHIFT_TABLE,
    S3_PARTITION_PREFIX,
    REGION_NAME,
    DELIVERIES_ATTEMPTS_COLUMNS
)

def extract_task(**context):
    extractor = DataExtractor(
        redshift_params=REDSHIFT_PARAMS,
        mongo_connection_string=MONGO_CONNECTION_STRING,
        mongo_database=MONGO_DATABASE,
        mongo_collection=MONGO_COLLECTION,
        s3_bucket_name=S3_BUCKET_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        etl_job_name=ETL_JOB_NAME,
        logger=logger,
        msg_text=MSG_TEXT,
    )
    extracted_date = extractor.run_extraction()
    
def transform_task(**context):

    transformer = DataTransformer(EGYPT_TZ, COLUMNS_TO_SELECT, MSG_TEXT, logger, DATA_TYPES, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME, S3_PARTITION_PREFIX, REGION_NAME)
    last_updated_at, s3_object_key = transformer.run_transformation()

    context['ti'].xcom_push(key='last_updated_at', value=last_updated_at)
    context['ti'].xcom_push(key='s3_object_key', value=s3_object_key)

def load_task(**context):
    last_updated_at = context['ti'].xcom_pull(key='last_updated_at')
    s3_object_key = context['ti'].xcom_pull(key='s3_object_key')
    
    logger.info(f"Last updated at: {last_updated_at}")
    logger.info(f"S3 object key: {s3_object_key}")
    loader = DataLoader(
        logger,
        REDSHIFT_PARAMS,
        S3_BUCKET_NAME,
        S3_PARTITION_PREFIX,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        REGION_NAME,
        MSG_TEXT,
        ETL_JOB_NAME,
        REDSHIFT_TABLE,
        DELIVERIES_ATTEMPTS_COLUMNS
    )
    loader.run_loading(last_updated_at, s3_object_key)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description="A DAG for extracting data",
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_extract = PythonOperator(
        task_id='run_extract',
        python_callable=extract_task,
        provide_context=True,
        dag=dag,
    )
    run_transform = PythonOperator(
        task_id='run_transform',
        python_callable=transform_task,
        provide_context=True,
        dag=dag,
    )

    run_load = PythonOperator(
        task_id='run_load',
        python_callable=load_task,
        provide_context=True,
        dag=dag,
    )
    run_extract >> run_transform >> run_load