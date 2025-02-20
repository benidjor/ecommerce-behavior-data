from airflow.decorators import task, dag
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import logging

# 로깅 설정
logger = logging.getLogger(__name__)

# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"
S3_BUCKET_NAME = "ecommerce-behavior-data"
LOCAL_FOLDER = "parquet_data/total_merged.parquet"
RAW_FOLDER = "raw-data"
PROCESSED_FOLDER = "processed-data"

# AWS 자격 증명 가져오기
aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
credentials = aws_hook.get_credentials()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_pipeline_using_spark",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Extract initial filtered data to S3",
)

application_args=[
    "--input_local_path", os.path.join(BASE_PATH, LOCAL_FOLDER),
    "--output_s3_raw_path", RAW_FOLDER,
    "--output_s3_processed_path", PROCESSED_FOLDER,
    "--aws_access_key", credentials.access_key,
    "--aws_secret_key", credentials.secret_key,
    "--start_date", "2019-10-01",
    "--end_date", "2019-10-28",
]

conf={
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "1g",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.access.key": credentials.access_key,
    "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
}

# SparkSubmitOperator를 직접 DAG 내부에서 선언
extract_and_upload_to_s3 = SparkSubmitOperator(
    task_id="extract_and_upload_to_s3",
    application="/opt/airflow/dags/initial_etl/extract_and_upload_to_s3.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

transform_and_upload_to_s3 = SparkSubmitOperator(
    task_id="transform_and_upload_to_s3",
    application="/opt/airflow/dags/initial_etl/transform.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

# DAG 실행
extract_and_upload_to_s3 >> transform_and_upload_to_s3
