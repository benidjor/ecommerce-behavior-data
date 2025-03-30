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
from initial_dags import initial_run_sql

# 로깅 설정
logger = logging.getLogger(__name__)

# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"
S3_BUCKET_NAME = "ecommerce-behavior-data"
LOCAL_FOLDER = "parquet_data/total_merged_parquet"
RAW_FOLDER = "raw-data"
PROCESSED_FOLDER = "processed-data"

# 테스트 모드 설정
TEST_MODE = True  # True면 20분 간격, False면 주간 실행

# 스케줄 간격 설정
schedule_interval = "*/20 * * * *" if TEST_MODE else "@weekly"

# 초기 데이터 시작 시점
START_DATE = datetime(2019, 11, 26)

# 강제로 현재 시간을 2023년 6월 11일로 설정 (Variable 사용)
CURRENT_DATE = Variable.get("current_date", default_var="2019-12-03")

RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable


# AWS 자격 증명 가져오기
aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
credentials = aws_hook.get_credentials()

def reset_variable(**kwargs):
    run_state = context["run_state"]
    dag_run = kwargs.get("dag_run")
    if dag_run:
        logger.info(f"DAG run_type: {dag_run.run_type}, RUN_STATE_FLAG: {run_state}")

     # 최초 실행(pending) 또는 강제 초기화(force_reset)인 경우 초기화 수행
    if run_state in ["pending", "force_reset"]:
        logger.info("Variable 및 S3 데이터 초기화를 수행합니다.")

        # Variable 초기화
        Variable.set("current_date", "2019-12-03")
        logger.info("Variable 초기화가 완료되었습니다.")
    else:
        logger.info("초기화 작업을 건너뜁니다. DAG가 이전에 이미 실행되었습니다.")

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

def update_variable():
    next_date = context["next_date"]
    
    Variable.set(RUN_STATE_FLAG, "done")
    Variable.set("current_date", next_date)
    logger.info(f"Variable 'current_date'가 {next_date}(으)로 업데이트 되었습니다.")


context = get_task_context()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": START_DATE
}

dag = DAG(
    "weekly_pipeline",
    default_args=default_args,
    schedule_interval=schedule_interval,
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

def copy_data_from_s3():
    """
    Snowflake ECOMMERCE_RAW_DATA Schema의 테이블에 통합 데이터셋 적재
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="copy_into_snowflake_from_s3.sql",
        schema=schema,
        context=context
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
        context=context
    )


def query_for_staging_or_analysis(schema):
    """
    PROCESSED_DATA Schema의 테이블을 활용하여, STAGING_DATA or ANALYSIS Schema에 테이블을 생성합니다.
    """
    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    
    if schema == "STAGING_DATA":
        initial_run_sql(
            initial_sql_filename="query_for_staging_data.sql",
            schema=schema,
            context=context
        )
    else:
        initial_run_sql(
            initial_sql_filename="query_for_analysis.sql",
            schema=schema,
            context=context
        )

# PythonOperator: Snowflake 데이터 로드 작업
reset_date_variable = PythonOperator(
    task_id="reset_date_variable",
    python_callable=reset_variable,
    dag=dag,
)

copy_raw_data_into_snowflake = PythonOperator(
    task_id="copy_raw_data_into_snowflake",
    python_callable=copy_data_from_s3,
    dag=dag,
)

# create_star_schema_tables_in_snowflake = PythonOperator(
#     task_id="create_star_schema_tables_in_snowflake",
#     python_callable=star_schema_tables,
#     dag=dag,
# )

insert_data_to_star_schema_tables_in_snowflake = PythonOperator(
    task_id="insert_data_to_star_schema_tables_in_snowflake",
    python_callable=insert_data_to_star_schema_tables,
    dag=dag,
)

create_tables_for_staging_data_schema_in_snowflake = PythonOperator(
    task_id="create_tables_for_staging_data_schema_in_snowflake",
    python_callable=query_for_staging_or_analysis,
    op_kwargs={"schema": "STAGING_DATA"},
    dag=dag,
)

create_tables_for_analysis_schema_in_snowflake = PythonOperator(
    task_id="create_tables_for_analysis_schema_in_snowflake",
    python_callable=query_for_staging_or_analysis,
    op_kwargs={"schema": "ANALYSIS"},
    dag=dag,
)

update_variable_operator = PythonOperator(
    task_id="update_variable",
    python_callable=update_variable,
    dag=dag,
)

# DAG 실행
reset_date_variable >> weekly_extract_and_upload_to_s3 >> weekly_transform_and_upload_to_s3 >> copy_raw_data_into_snowflake >>  insert_data_to_star_schema_tables_in_snowflake >> create_tables_for_staging_data_schema_in_snowflake >> create_tables_for_analysis_schema_in_snowflake >> update_variable_operator