import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# from initial_pipeline.extract import main as extract_main
# from initial_pipeline.transform import main as transform_main

from config.data_filtering_config import BASE_PATH, NEW_COLUMN_ORDER, INITIAL_CONTEXT
from config.s3_config import S3_BUCKET_NAME, LOCAL_FOLDER, RAW_FOLDER, PROCESSED_FOLDER

from initial_pipeline.load import (
    reset_tables, copy_data_from_s3, star_schema_tables, insert_data_to_star_schema_tables, query_for_staging_or_analysis
)

logger = logging.getLogger(__name__)

# 기본 경로 설정
# BASE_PATH = "/opt/airflow/data"
# S3_BUCKET_NAME = "ecommerce-behavior-data"
# LOCAL_FOLDER = "parquet_data/total_merged_parquet"
# RAW_FOLDER = "raw-data"
# PROCESSED_FOLDER = "processed-data"

# AWS 자격 증명 가져오기
aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
credentials = aws_hook.get_credentials()

# context = {
#     "start_date": "2019-10-01",
#     "end_date": "2019-11-25"
# }

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "2",  # 고정 Executor 2개 사용
    "spark.driver.memory": "2g",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.access.key": credentials.access_key,
    "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
}

spark_jars = "/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar"


with DAG(
    dag_id="initial_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    # start_date=datetime(2019, 10, 1),
    description="Initial Tasks Pipeline for ETL in S3 & ELT in Snowflake"
) as dag:

    # 1. Extract
    extract_task = SparkSubmitOperator(
        task_id="initial_extract",
        application=os.path.join(os.path.dirname(__file__), "extract.py"),
        conn_id="spark_default",
        application_args=application_args,
        conf=conf,
        jars=spark_jars,
    )

    # 2. Transform
    transform_task = SparkSubmitOperator(
        task_id="initial_transform",
        application=os.path.join(os.path.dirname(__file__), "transform.py"),
        conn_id="spark_default",
        application_args=application_args,
        conf=conf,
        jars=spark_jars,
    )

    # 3. Load (Snowflake)
    reset_tables_in_snowflake = PythonOperator(
        task_id="reset_tables", 
        python_callable=reset_tables
    )

    copy_raw_data_into_snowflake = PythonOperator(
        task_id="copy_raw_data",
        python_callable=copy_data_from_s3
    )

    create_star_schema_tables_in_snowflake = PythonOperator(
        task_id="create_star_schema", 
        python_callable=star_schema_tables
    )

    insert_data_to_star_schema_tables_in_snowflake = PythonOperator(
        task_id="insert_data_to_star_schema_tables_in_snowflake",
        python_callable=insert_data_to_star_schema_tables,
        op_kwargs={"context": INITIAL_CONTEXT}
    )

    create_tables_for_staging_data_schema_in_snowflake = PythonOperator(
        task_id="create_tables_for_staging_data_schema_in_snowflake",
        python_callable=query_for_staging_or_analysis,
        op_kwargs={"schema": "STAGING_DATA", "context": INITIAL_CONTEXT},
        dag=dag,
    )

    create_tables_for_analysis_schema_in_snowflake = PythonOperator(
        task_id="create_tables_for_analysis_schema_in_snowflake",
        python_callable=query_for_staging_or_analysis,
        op_kwargs={"schema": "ANALYSIS", "context": INITIAL_CONTEXT},
        dag=dag,
    )


    # DAG 의존성
    extract_task >> transform_task \
        >> reset_tables_in_snowflake >> copy_raw_data_into_snowflake \
            >> create_star_schema_tables_in_snowflake >> insert_data_to_star_schema_tables_in_snowflake \
            >> create_tables_for_staging_data_schema_in_snowflake >> create_tables_for_analysis_schema_in_snowflake
