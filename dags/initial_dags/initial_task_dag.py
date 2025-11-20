from airflow.decorators import task, dag
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import logging

# load_to_snowflake.py 파일에서 Snowflake 관련 함수들을 import
from initial_etl import initial_run_sql

# 로깅 설정
logger = logging.getLogger(__name__)

# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"
S3_BUCKET_NAME = "ecommerce-behavior-data"
LOCAL_FOLDER = "parquet_data/total_merged_parquet"
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
    "initial_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Initial Tasks for ETL in S3 & ELT in Snowflake",
)

application_args=[
    "--input_local_path", os.path.join(BASE_PATH, LOCAL_FOLDER),
    "--output_s3_raw_path", RAW_FOLDER,
    "--output_s3_processed_path", PROCESSED_FOLDER,
    "--aws_access_key", credentials.access_key,
    "--aws_secret_key", credentials.secret_key,
    "--start_date", "2019-10-01",
    "--end_date", "2019-11-25",
]

conf = {
    "spark.executor.memory": "6g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "2",  # 고정 Executor 2개 사용
    "spark.driver.memory": "1g",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.access.key": credentials.access_key,
    "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
}


# SparkSubmitOperator를 직접 DAG 내부에서 선언
initial_extract_and_upload_to_s3 = SparkSubmitOperator(
    task_id="initial_extract_and_upload_to_s3",
    application="/opt/airflow/dags/initial_dags/initial_extract_and_upload_to_s3.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

initial_transform_and_upload_to_s3 = SparkSubmitOperator(
    task_id="initial_transform_and_upload_to_s3",
    application="/opt/airflow/dags/initial_dags/initial_transform_and_load_to_s3.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

def reset_tables():
    """
    Snowflake ECOMMERCE_RAW_DATA Schema에 통합 데이터셋 적재할 테이블 생성
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="reset_tables_for_initial_raw_data.sql",
        schema=schema,
    )

def copy_data_from_s3():
    """
    Snowflake ECOMMERCE_RAW_DATA Schema의 테이블에 통합 데이터셋 적재
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="copy_into_snowflake_for_initial_raw_data.sql",
        schema=schema,
    )

def star_schema_tables():
    """
    Star Schema에 기반하여, Snowflake ECOMMERCE_PROCESSED_DATA Schema에 테이블 생성
    """
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="star_schema_for_initial_processed_data.sql",
        schema=schema,
    )

def insert_data_to_star_schema_tables():
    """
    SnowflakeRaw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재
    """
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="insert_into_initial_processed_data.sql",
        schema=schema,
    )


# PythonOperator: Snowflake 테이블 리셋 작업
reset_tables_in_snowflake = PythonOperator(
    task_id="reset_tables_in_snowflake",
    python_callable=reset_tables,
    dag=dag,
)

# PythonOperator: Snowflake 데이터 로드 작업
copy_raw_data_into_snowflake = PythonOperator(
    task_id="copy_raw_data_into_snowflake",
    python_callable=copy_data_from_s3,
    dag=dag,
)

# PythonOperator: Snowflake 데이터 로드 작업
create_star_schema_tables_in_snowflake = PythonOperator(
    task_id="create_star_schema_tables_in_snowflake",
    python_callable=star_schema_tables,
    dag=dag,
)

# PythonOperator: Snowflake 데이터 로드 작업
insert_data_to_star_schema_tables_in_snowflake = PythonOperator(
    task_id="insert_data_to_star_schema_tables_in_snowflake",
    python_callable=insert_data_to_star_schema_tables,
    dag=dag,
)

# DAG 실행
initial_extract_and_upload_to_s3 >> transform_and_upload_to_s3 >> reset_tables_in_snowflake >> copy_raw_data_into_snowflake >> create_star_schema_tables_in_snowflake >> insert_data_to_star_schema_tables_in_snowflake
