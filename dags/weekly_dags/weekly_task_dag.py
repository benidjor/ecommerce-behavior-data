from airflow.decorators import task, dag
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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

# 테스트 모드 설정
TEST_MODE = True  # True면 10분 간격, False면 주간 실행

# 스케줄 간격 설정
schedule_interval = "*/15 * * * *" if TEST_MODE else "@weekly"

# 초기 데이터 시작 시점
START_DATE = datetime(2019, 11, 26)

# 강제로 현재 시간을 2023년 6월 11일로 설정 (Variable 사용)
CURRENT_DATE = Variable.get("current_date", default_var="2019-12-02")

RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable


# AWS 자격 증명 가져오기
aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
credentials = aws_hook.get_credentials()

def get_task_context():
    """
    DAG 실행을 위한 컨텍스트를 반환합니다.
    Airflow에서 컨텍스트: 태스크 실행과 관련된 정보의 집합 (DAG 실행 정보, 환경 변수, 사용자 정의 데이터 등)
    """

    run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")
    forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    start_date = (START_DATE + timedelta(weeks=((forced_date - START_DATE).days // 7) - 1)).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    next_date = (datetime.strptime(CURRENT_DATE, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d")

    return {
        "run_state": run_state,
        "forced_date": forced_date,
        "start_date": start_date,
        "end_date": end_date,
        "next_date": next_date
    }

context = get_task_context()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weekly_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Weekly Tasks for ETL in S3 & ELT in Snowflake",
)

application_args=[
    "--input_local_path", os.path.join(BASE_PATH, LOCAL_FOLDER),
    "--output_s3_raw_path", RAW_FOLDER,
    "--output_s3_processed_path", PROCESSED_FOLDER,
    "--aws_access_key", credentials.access_key,
    "--aws_secret_key", credentials.secret_key,
    "--start_date", context["start_date"],
    "--end_date", context["end_date"],
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
weekly_extract_and_upload_to_s3 = SparkSubmitOperator(
    task_id="weekly_extract_and_upload_to_s3",
    application="/opt/airflow/dags/weekly_dags/weekly_extract_and_upload_to_s3.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

weekly_transform_and_upload_to_s3 = SparkSubmitOperator(
    task_id="transform_and_upload_to_s3",
    application="/opt/airflow/dags/weekly_dags/weekly_transform_and_load_to_s3.py",
    conn_id="spark_default",
    application_args=application_args,
    conf=conf,
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

