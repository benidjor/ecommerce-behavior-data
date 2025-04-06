import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# refactored plugins의 task 함수 import
from custom_modules.config.s3_config import S3_BUCKET_NAME, LOCAL_FOLDER, RAW_FOLDER, PROCESSED_FOLDER
from custom_modules.utils.variable_helpers import get_task_context, reset_variable, update_variable
from custom_modules.tasks.initial_tasks import run_initial_extract_task
from custom_modules.tasks.spark_tasks import run_spark_transformation_task
from custom_modules.tasks.snowflake_tasks import execute_sql_task

# 로깅 설정
logger = logging.getLogger(__name__)

# 초기 데이터 시작 시점
START_DATE = datetime(2019, 11, 26)

# 테스트 모드 설정
TEST_MODE = True  # True면 30분 간격, False면 주간 실행

# get_task_context()에서 가져오는 변수를 통해 날짜 정보 가져오기
# context = get_task_context()

# 스케줄 간격 설정
schedule_interval = "*/30 * * * *" if TEST_MODE else "@weekly"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": START_DATE
}

@dag(
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    description="Weekly Tasks for ETL in S3 & ELT in Snowflake"
)


def weekly_pipeline_dag():
    # AWS 자격 증명 가져오기
    aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
    credentials = aws_hook.get_credentials()
    
    input_local_path = os.path.join("/opt/airflow/data", LOCAL_FOLDER)
    output_s3_raw_path = RAW_FOLDER
    output_s3_processed_path = PROCESSED_FOLDER
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key

    # 컨텍스트 생성 (시작일, 종료일, 다음 날짜 등)
    task_context = get_task_context()
    start_date = task_context["start_date"]
    end_date = task_context["end_date"]

    @task
    def reset_variables_task(**kwargs):
        reset_variable(**kwargs)
        return "Reset Completed"

    reset_variable_task = reset_variables_task()

    weekly_extract_and_upload_to_s3 = run_initial_extract_task(
        input_local_path=input_local_path,
        output_s3_raw_path=output_s3_raw_path,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        start_date=start_date,
        end_date=end_date
    )

    weekly_transform_and_upload_to_s3 = run_spark_transformation_task(
        input_raw_s3_path=output_s3_raw_path,
        output_s3_processed_path=output_s3_processed_path,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        start_date=start_date,
        end_date=end_date
    )
    
    # Snowflake ECOMMERCE_RAW_DATA Schema의 테이블에 통합 데이터셋 적재
    copy_data_task = execute_sql_task.override(task_id="copy_raw_data_into_snowflake")("copy_into_snowflake_from_s3.sql", schema="RAW_DATA", context=task_context)

    # SnowflakeRaw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재
    insert_data_task = execute_sql_task.override(task_id="insert_data_to_star_schema_tables_in_snowflake")("weekly_insert_into_initial_processed_data.sql", schema="PROCESSED_DATA", context=task_context)

    #  PROCESSED_DATA Schema의 테이블을 활용하여, STAGING_DATA or ANALYSIS Schema에 테이블을 생성
    create_staging_data_schema_task = execute_sql_task.override(task_id="create_tables_for_staging_data_schema_in_snowflake")("weekly_query_for_staging_data.sql", schema="PROCESSED_DATA", context=task_context)

    create_analysis_schema_task = execute_sql_task.override(task_id="create_tables_for_analysis_schema_in_snowflake")("weekly_query_for_analysis.sql", schema="PROCESSED_DATA", context=task_context)

    @task
    def update_variables_task():
        update_variable()
        return "Variable Updated"

    update_variable_task = update_variables_task()

    # 작업 순서: reset_variable -> extract -> transform -> SQL 작업 -> update_variable
    reset_variable_task >> weekly_extract_and_upload_to_s3 >> weekly_transform_and_upload_to_s3 >> copy_data_task >> insert_data_task >> create_staging_data_schema_task >> create_analysis_schema_task >> update_variable_task

dag_instance = weekly_pipeline_dag()
