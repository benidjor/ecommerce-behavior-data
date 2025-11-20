import os
import logging
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# from weekly_pipeline.extract import main as extract_main
# from weekly_pipeline.transform import main as transform_main

from config.data_filtering_config import BASE_PATH, NEW_COLUMN_ORDER
from config.s3_config import S3_BUCKET_NAME, LOCAL_FOLDER, RAW_FOLDER, PROCESSED_FOLDER
from common_utils.variable_utils import START_DATE, RUN_STATE_FLAG

from common_utils.date_utils import reset_variable, get_task_context, update_variable
from weekly_pipeline.load import copy_data_from_s3, insert_data_to_star_schema_tables, query_for_staging_or_analysis


logger = logging.getLogger(__name__)

# 테스트 모드 설정
TEST_MODE = True  # True면 30분 간격, False면 주간 실행

# 스케줄 간격 설정
schedule_interval = "*/30 * * * *" if TEST_MODE else "@weekly"

# # 초기 데이터 시작 시점
# START_DATE = datetime(2019, 11, 26)

# # 강제로 현재 시간을 2023년 12월 03일로 설정 (Variable 사용)
CURRENT_DATE = Variable.get("current_date", default_var="2019-12-03")

# RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable


# AWS 자격 증명 가져오기
aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
credentials = aws_hook.get_credentials()

context_filter = get_task_context(RUN_STATE_FLAG, CURRENT_DATE, START_DATE)

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
    "--start_date", context_filter["start_date"],
    "--end_date", context_filter["end_date"],
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
    dag_id="weekly_pipeline",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    start_date=START_DATE,
    description="Weekly Tasks for ETL in S3 & ELT in Snowflake"
) as dag:

    # 1. Variable 초기화
    reset_date_variable = PythonOperator(
        task_id="reset_date_variable",
        python_callable=reset_variable,
        op_kwargs={"context": context_filter},
        dag=dag
    )

    # 2. Extract
    extract_task = SparkSubmitOperator(
        task_id="weekly_extract",
        application=os.path.join(os.path.dirname(__file__), "extract.py"),
        conn_id="spark_default",
        application_args=application_args,
        conf=conf,
        jars=spark_jars,
    )

    # 3. Transform
    transform_task = SparkSubmitOperator(
        task_id="weekly_transform",
        application=os.path.join(os.path.dirname(__file__), "transform.py"),
        conn_id="spark_default",
        application_args=application_args,
        conf=conf,
        jars=spark_jars,
    )

    # 4. Load → RAW COPY
    # copy_raw_data_into_snowflake = PythonOperator(
    #     task_id="copy_raw_data",
    #     python_callable=copy_data_from_s3,
    #     op_kwargs={"context": context}
    # )

    copy_raw_data_into_snowflake = PythonOperator(
        task_id="copy_raw_data",
        python_callable=copy_data_from_s3,
        dag=dag,
    )

    # 5. Load → PROCESSED INSERT
    insert_data_to_star_schema_tables_in_snowflake = PythonOperator(
        task_id="insert_data_to_star_schema_tables_in_snowflake",
        python_callable=insert_data_to_star_schema_tables,
        op_kwargs={"context": context_filter}
    )

    # 6. Load → STAGING & ANALYSIS
    create_tables_for_staging_data_schema_in_snowflake = PythonOperator(
        task_id="create_tables_for_staging_data_schema_in_snowflake",
        python_callable=query_for_staging_or_analysis,
        op_kwargs={"schema": "STAGING_DATA", "context": context_filter},
        dag=dag,
    )
    
    create_tables_for_analysis_schema_in_snowflake = PythonOperator(
        task_id="create_tables_for_analysis_schema_in_snowflake",
        python_callable=query_for_staging_or_analysis,
        op_kwargs={"schema": "ANALYSIS", "context": context_filter},
        dag=dag,
    )

    # 7. Variable 업데이트
    update_variable_operator = PythonOperator(
        task_id="update_variable",
        python_callable=update_variable,
        op_kwargs={"context": context_filter, "RUN_STATE_FLAG":RUN_STATE_FLAG},
        dag=dag,
    )

    # 의존성 정의
    reset_date_variable \
        >> extract_task >> transform_task \
            >> copy_raw_data_into_snowflake >> insert_data_to_star_schema_tables_in_snowflake\
            >> create_tables_for_staging_data_schema_in_snowflake >> create_tables_for_analysis_schema_in_snowflake\
            >> update_variable_operator