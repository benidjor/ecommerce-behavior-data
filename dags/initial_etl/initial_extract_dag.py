from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, count, sum, round, countDistinct, lit, coalesce, row_number, to_date, date_format
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
from io import BytesIO
import logging
from functools import reduce
import psutil # 메모리 및 CPU 사용량 확인을 위한 라이브러리
import pyarrow.parquet as pq

# 로깅 설정
logger = logging.getLogger(__name__)

# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"
S3_BUCKET_NAME = "ecommerce-behavior-data"
RAW_FOLDER = "raw-data"

# def get_spark_session(aws_access_key, aws_secret_key):
def get_spark_session():

    """
    SparkSession을 생성하거나 기존 세션을 가져옴
    - AWS S3와의 연결 설정 포함
    """
    aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
    credentials = aws_hook.get_credentials()
    spark = SparkSession.builder \
        .appName("Filter & Transform Data") \
        .config("spark.hadoop.fs.s3a.access.key", credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Spark 로그는 WARN 이상만 출력

    return spark

def set_filtering_date(start_date: str="2019-10-01", end_date: str="2020-01-04", freq: str="7D") -> dict:
    weekly_dict = {
        f"week_{i+1}": {
            "start_date": str(start.date()),
            "end_date": str((start + pd.Timedelta(days=6)).date())
        }
        for i, start in enumerate(pd.date_range(start=start_date, end=end_date, freq=freq))
    }

    return weekly_dict

def filter_by_date(spark, dir1: str="parquet_data", dir2: str="2019_oct.parquet", start_date: str="2019-10-01", end_date: str="2020-01-04") -> SparkDataFrame:

    base_path = os.path.join("/opt/airflow/data", dir1, dir2)

    try:
        df = spark.read.parquet(base_path)
        if df.isEmpty():
            logger.warning(f"데이터프레임이 비어 있습니다.")
            return None
        
        # 날짜 및 시간 컬럼 필터링
        ymd_column = [col for col in df.columns if col.endswith('ymd')]
        min_column = [col for col in df.columns if col.endswith('min')]

        # 날짜 및 시간 변환
        for col in ymd_column + min_column:
            df = df.withColumn(col, to_date(df[col]))

        logger.info(f"{df}의 필터링 기준 컬럼: {ymd_column[0]}")
        filtered_df = df.filter(to_date(df[ymd_column[0]]).between(start_date, end_date))

        return filtered_df

    except Exception as e:
        logger.error(f"파일 처리 중 오류가 발생했습니다: {e}")
        return None

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_hook = AwsBaseHook(aws_conn_id='AWS_CONNECTION_ID', client_type='s3')
    credentials = aws_hook.get_credentials()

    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

def upload_to_s3(df: SparkDataFrame, start_date: str):
    """
    Spark DataFrame을 직접 S3에 Parquet 형식으로 저장하는 함수
    """
    # 1️. 종료 날짜 계산
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

    # 2️. S3 저장 경로 설정 (YYMMDD 형식)
    transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    s3_path = f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/{transformed_key}/"

    try:
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 직접 저장합니다 ({s3_path})")

        # 3️. Spark DataFrame을 S3에 직접 저장
        df.write.mode("overwrite").parquet(s3_path)

        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")

    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)



def making_columns_for_transform(df: SparkDataFrame, event_col: str) -> SparkDataFrame:
    """
    event_time 컬럼을 바탕으로, 월, 일, 시, 요일 파생 컬럼 생성
    """
    transformations = {
        "event_time_month": "MM",      # 월
        "event_time_day": "dd",        # 일
        "event_time_hour": "HH",       # 시
        "event_time_day_name": "E"     # 요일
    }

    df = reduce(lambda acc, col_format: acc.withColumn(col_format[0], date_format(event_col, col_format[1])),
                transformations.items(), df)

    return df


@task
def process_filtered_data():

    spark = get_spark_session()
    weekly_dict = set_filtering_date(start_date="2019-10-01", end_date="2019-10-13", freq="7D")
    for week, date_range in weekly_dict.items():
        start_date = date_range["start_date"]
        end_date = date_range["end_date"]

        # 1주일 단위로 데이터 필터링
        filtered_data = filter_by_date(spark, "parquet_data", "2019_oct.parquet", start_date, end_date)

        # 필터링된 데이터 S3에 업로드
        if filtered_data:
            upload_to_s3(filtered_data, start_date)

@dag(
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    description="Extract initial filtered data to S3",
)

def filter_initial_extract_dag():
    
    process_filtered_data()

dag = filter_initial_extract_dag()