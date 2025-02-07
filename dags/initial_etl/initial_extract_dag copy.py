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
LOCAL_FOLDER = "parquet_data/2019_oct.parquet"
RAW_FOLDER = "raw-data"

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
    "extract_with_sparksubmitoperator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Extract initial filtered data to S3",
)

# SparkSubmitOperator를 직접 DAG 내부에서 선언
spark_submit_task = SparkSubmitOperator(
    task_id="filter_transform",
    application="/opt/airflow/dags/initial_etl/filter_transform.py",
    conn_id="spark_default",
    application_args=[
        "--input_local_path", os.path.join(BASE_PATH, LOCAL_FOLDER),
        "--output_s3_raw_path", RAW_FOLDER,
        "--aws_access_key", credentials.access_key,
        "--aws_secret_key", credentials.secret_key,
        "--start_date", "2019-10-01",
        "--end_date", "2019-10-14",
    ],
    conf={
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.hadoop.fs.s3a.access.key": credentials.access_key,
        "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
    },
    jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
    dag=dag,
)

# DAG 실행
spark_submit_task


# # 주간 필터링 날짜 생성
# def generate_weekly_dates(start_date="2019-10-01",end_date = "2020-01-04", freq = "7D"):

#     week_tuples = [
#         (str(start.date()), str((start + timedelta(days=6)).date()))
#         for start in pd.date_range(start=start_date, end=end_date, freq=freq)
#     ]
    
#     return week_tuples

# @dag(
#     default_args={
#         "owner": "airflow",
#         "depends_on_past": False,
#         "retries": 1,
#         "retry_delay": timedelta(minutes=5),
#     },
#     schedule_interval=None,
#     start_date=datetime(2023, 6, 1),
#     catchup=False,
#     description="Extract initial filtered data to S3",
# )

# def extract_with_sparksubmitoperator():

#     @task
#     def process_filtered_data():

#         spark_submit_task = SparkSubmitOperator(
#             task_id="filter_transform",
#             application="/opt/airflow/dags/filter_transform.py",
#             conn_id="spark_default",
#             application_args=[
#                 "--input_local_path", f"{BASE_PATH}/{LOCAL_FOLDER}",
#                 "--output_s3_raw_path", f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/",
#                 # "--aws_access_key", credentials.access_key,
#                 # "--aws_secret_key", credentials.secret_key,
#                 "--start_date", "2019-10-01",
#                 "--end_date", "2019-10-14",
#             ],
#             conf={
#                     "spark.executor.memory": "2g",
#                     "spark.executor.cores": "2",
#                     "spark.driver.memory": "1g",
#                     # S3A 파일시스템 설정 추가
#                     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#                     "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
#                     "spark.hadoop.fs.s3a.access.key": credentials.access_key,
#                     "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
#             },
#             jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
#         )

#         # spark_result = spark_submit_task.execute(context={})

#         return spark_submit_task

#     process_filtered_data()

# dag = extract_with_sparksubmitoperator()

    # spark = get_spark_session()
    # weekly_dict = set_filtering_date(start_date="2019-10-01", end_date="2019-10-13", freq="7D")
    # for week, date_range in weekly_dict.items():
    #     start_date = date_range["start_date"]
    #     end_date = date_range["end_date"]

    #     # 1주일 단위로 데이터 필터링
    #     filtered_data = filter_by_date(spark, "parquet_data", "2019_oct.parquet", start_date, end_date)

    #     # 필터링된 데이터 S3에 업로드
    #     if filtered_data:
    #         upload_to_s3(filtered_data, start_date)




# weekly_dates = generate_weekly_dates()
# spark_tasks = []
# for start_date, end_date in weekly_dates:
#     spark_task = SparkSubmitOperator(
#         task_id=f"filter_and_upload_{start_date}",
#         application="/opt/airflow/dags/filter_transform.py",
#         conn_id="spark_default",  # Spark 연결 ID
#         application_args=[
#             "--input_path", "/opt/airflow/data/parquet_data/2019_oct.parquet",
#             "--start_date", start_date,
#             "--end_date", end_date,
#             "--output_folder", "raw-data"
#         ],
#         conf={
#             "spark.executor.memory": "2g",
#             "spark.hadoop.fs.s3a.access.key": credentials.access_key,
#             "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
#             "spark.hadoop.fs.s3a.session.token": credentials.token,
#         },
#         dag=dag,
#     )
#     spark_tasks.append(spark_task)

# spark_tasks