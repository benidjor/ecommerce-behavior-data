# dags/initial_dag/initial_task_dag.py
import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from custom_modules.config.s3_config import S3_BUCKET_NAME, LOCAL_FOLDER, RAW_FOLDER, PROCESSED_FOLDER
from custom_modules.tasks.initial_tasks import run_initial_extract_task
from custom_modules.tasks.spark_tasks import run_spark_transformation_task
from custom_modules.tasks.snowflake_tasks import execute_sql_task

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False,
    description="Initial Tasks for ETL in S3 & ELT in Snowflake"
)

def initial_pipeline_dag():

    # AWS 자격 증명 가져오기
    aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
    credentials = aws_hook.get_credentials()
    
    input_local_path = os.path.join("/opt/airflow/data", LOCAL_FOLDER)
    output_s3_raw_path = RAW_FOLDER
    output_s3_processed_path = PROCESSED_FOLDER
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    start_date = "2019-10-01"
    end_date = "2019-11-25"
    
    extract_task = run_initial_extract_task(input_local_path,
                                            output_s3_raw_path,
                                            aws_access_key,
                                            aws_secret_key,
                                            start_date,
                                            end_date)
    
    transform_task = run_spark_transformation_task(input_raw_s3_path=output_s3_raw_path,
                                                   output_s3_processed_path=output_s3_processed_path,
                                                   aws_access_key=aws_access_key,
                                                   aws_secret_key=aws_secret_key,
                                                   start_date=start_date,
                                                   end_date=end_date)
    
    
    reset_tables_task = execute_sql_task.override(task_id="reset_tables_in_snowflake")("reset_tables_for_initial_raw_data.sql", schema="RAW_DATA", context={"start_date": start_date, "end_date": end_date})

    # Snowflake ECOMMERCE_RAW_DATA Schema의 테이블에 통합 데이터셋 적재
    copy_data_task = execute_sql_task.override(task_id="copy_raw_data_into_snowflake")("copy_into_snowflake_from_s3.sql", schema="RAW_DATA", context={"start_date": start_date, "end_date": end_date})

    star_schema_task = execute_sql_task.override(task_id="create_star_schema_tables_in_snowflake")("star_schema_for_initial_processed_data.sql", schema="PROCESSED_DATA", context={"start_date": start_date, "end_date": end_date})
    
    # SnowflakeRaw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재
    insert_data_task = execute_sql_task.override(task_id="insert_data_to_star_schema_tables_in_snowflake")("insert_into_initial_processed_data.sql", schema="PROCESSED_DATA", context={"start_date": start_date, "end_date": end_date})

    # PROCESSED_DATA Schema의 테이블을 활용하여, STAGING_DATA or ANALYSIS Schema에 테이블을 생성
    create_staging_data_schema_task = execute_sql_task.override(task_id="create_tables_for_staging_data_schema_in_snowflake")("query_for_staging_data.sql", schema="PROCESSED_DATA", context={"start_date": start_date, "end_date": end_date})

    create_analysis_schema_task = execute_sql_task.override(task_id="create_tables_for_analysis_schema_in_snowflake")("query_for_analysis.sql", schema="PROCESSED_DATA", context={"start_date": start_date, "end_date": end_date})

    
    # 작업 순서 정의
    extract_task >> transform_task >> reset_tables_task >> copy_data_task >> star_schema_task >> insert_data_task >> create_staging_data_schema_task >> create_analysis_schema_task

dag_instance = initial_pipeline_dag()