from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format
from datetime import datetime, timedelta
import pandas as pd
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import argparse
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET_NAME = "ecommerce-behavior-data"
RAW_FOLDER = "raw-data"

def get_spark_session(aws_access_key, aws_secret_key):
    """
    AWS Connection ID를 사용하여 AWS 자격 증명을 가져온 후 SparkSession을 생성
    """
    # aws_hook = AwsBaseHook(aws_conn_id="aws_connection_id", client_type="s3")
    # credentials = aws_hook.get_credentials()

    spark = SparkSession.builder \
        .appName("Extract & Transform Data") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark

def set_filtering_date(weekly_start_date: str="2019-10-01", weekly_end_date: str="2020-01-04", freq: str="7D") -> dict:
    weekly_dict = {
        f"week_{i+1}": {
            "weekly_start_date": str(start.date()),
            "weekly_end_date": str((start + pd.Timedelta(days=6)).date())
        }
        for i, start in enumerate(pd.date_range(start=weekly_start_date, end=weekly_end_date, freq=freq))
    }

    return weekly_dict

def filter_by_date(spark, input_local_path, start_date, end_date):
    """
    특정 날짜 범위로 데이터를 필터링
    """
    logger.info(f"로컬 디렉토리: {input_local_path}")

    try:
        df = spark.read.parquet(input_local_path)

        if df.isEmpty():
            logger.warning(f"데이터프레임이 비어 있습니다.")
            return None

        ymd_column = [col for col in df.columns if col.endswith('ymd')]
        for col in ymd_column:
            df = df.withColumn(col, to_date(df[col]))

        logger.info(f"{df}의 필터링 기준 컬럼: {ymd_column[0]}")
        filtered_df = df.filter(df[ymd_column[0]].between(start_date, end_date))

        return filtered_df

    except Exception as e:
        logger.error(f"파일 처리 중 오류 발생: {e}")
        return None

def making_columns_for_transform(df):
    """
    event_time 컬럼을 바탕으로, 월, 일, 시, 요일 파생 컬럼 생성
    """
    transformations = {
        "event_time_month": "MM",       # 월
        "event_time_day": "dd",         # 일
        "event_time_hour": "HH",        # 시
        "event_time_day_name": "E"      # 요일
    }

    for col_name, format_str in transformations.items():
        df = df.withColumn(col_name, date_format("event_time", format_str))

    return df

def upload_to_s3(df, start_date, output_s3_raw_path):
    """
    Spark DataFrame을 직접 S3에 Parquet 형식으로 저장하는 함수
    """
    # 1️. 종료 날짜 계산
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

    # 2️. S3 저장 경로 설정 (YYMMDD 형식)
    transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    s3_path = f"s3a://{S3_BUCKET_NAME}/{output_s3_raw_path}/{transformed_key}/"

    try:
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        # 3️. Spark DataFrame을 S3에 직접 저장
        df.write.mode("overwrite").parquet(s3_path)
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")

    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)


def main(input_local_path, output_s3_raw_path, aws_access_key, aws_secret_key, start_date, end_date):

    spark = get_spark_session(aws_access_key, aws_secret_key)

    weekly_dict = set_filtering_date(weekly_start_date=start_date, weekly_end_date=end_date, freq="7D")

    for week, date_range in weekly_dict.items():
        weekly_start_date = date_range["weekly_start_date"]
        weekly_end_date = date_range["weekly_end_date"]

        # 1주일 단위로 데이터 필터링
        filtered_df = filter_by_date(spark, input_local_path, weekly_start_date, weekly_end_date)

        # 필터링된 데이터 S3에 업로드
        if filtered_df:
            transformed_df = making_columns_for_transform(filtered_df)
            upload_to_s3(transformed_df, weekly_start_date, output_s3_raw_path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Spark 데이터 필터링 및 S3 업로드")
    parser.add_argument("--input_local_path", type=str, required=True, help="입력 데이터 Parquet 파일 경로")
    parser.add_argument("--output_s3_raw_path", type=str, required=True, help="S3 업로드 폴더명")
    parser.add_argument("--aws_access_key", required=True, help="AWS Access Key")
    parser.add_argument("--aws_secret_key", required=True, help="AWS Secret key")
    parser.add_argument("--start_date", type=str, required=True, help="필터링 시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="필터링 종료 날짜 (YYYY-MM-DD)")
    
    args = parser.parse_args()

    main(args.input_local_path, args.output_s3_raw_path, args.aws_access_key, args.aws_secret_key, args.start_date, args.end_date)
