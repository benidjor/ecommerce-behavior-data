from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format
from datetime import datetime, timedelta
import pandas as pd
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import argparse
import logging
import boto3

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET_NAME = "ecommerce-behavior-data"
RAW_FOLDER = "raw-data"

def get_spark_session(aws_access_key, aws_secret_key):
    """
    AWS Connection ID를 사용하여 AWS 자격 증명을 가져온 후 SparkSession을 생성
    """

    spark = SparkSession.builder \
        .appName("Extract & Transform Data") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # SparkSession Time Zone을 UTC로 설정
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    logger.info(f"Current TimeZone: {spark.conf.get("spark.sql.session.timeZone")}")

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

        logger.info(f"필터링 기준 컬럼: {ymd_column[0]}")
        filtered_df = df.filter(df[ymd_column[0]].between(start_date, end_date))

        return filtered_df

    except Exception as e:
        logger.error(f"파일 처리 중 오류 발생: {e}")
        return None


def detect_date_format(date_str):
    """ 주어진 날짜가 'YYYY-MM-DD'인지 'YYMMDD'인지 판별 후 datetime 객체로 변환 """
    try:
        # YYYY-MM-DD 형식일 경우
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        pass  # 다음 형식 검사

    try:
        # YYMMDD 형식일 경우
        return datetime.strptime(date_str, "%y%m%d")
    except ValueError:
        raise ValueError(f"올바른 날짜 형식이 아닙니다: {date_str} (YYYY-MM-DD 또는 YYMMDD만 허용됨)")


def s3_path_exists(bucket_name, prefix, aws_access_key, aws_secret_key):
    """ S3 경로가 존재하는지 확인하는 함수 """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return 'Contents' in response  # 객체가 존재하면 True


# def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key):
#     """
#     Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
#     해당 경로가 존재하면 저장을 건너뛴다.
#     """
#     # 1️. start_date의 형식 자동 감지 및 datetime 변환
#     date_obj = detect_date_format(start_date)

#     # 2️. 종료 날짜 계산
#     end_date = (date_obj + timedelta(days=6)).strftime("%Y-%m-%d")

#     # 3. S3 저장 경로 설정 (YYMMDD 형식)
#     transformed_key = date_obj.strftime("%y%m%d")  # 무조건 YYMMDD 형식으로 변환

#     s3_prefix = f"{output_s3_raw_path}/{transformed_key}/"

#     # 4. S3 경로 존재 여부 확인
#     if s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
#         logger.info(f"S3 경로가 이미 존재하므로 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
#         return

#     try:
#         # 5. Spark DataFrame을 S3에 직접 저장
#         s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
#         df.write.mode("overwrite").parquet(s3_path)
        
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")
    
#     except Exception as e:
#         logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)

def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key, is_raw=True, schema_key=None):
    """
    Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
    해당 경로가 존재하면 저장을 건너뜀
    """
    # 1️. start_date의 형식 자동 감지 및 datetime 변환
    date_obj = detect_date_format(start_date)

    # 2️. 종료 날짜 계산
    end_date = (date_obj + timedelta(days=6)).strftime("%Y-%m-%d")

    # 3. S3 저장 경로 설정 (YYMMDD 형식)
    transformed_key = date_obj.strftime("%y%m%d")  # 무조건 YYMMDD 형식으로 변환

    if is_raw:
        s3_prefix = f"{output_s3_raw_path}/{transformed_key}/"
    else:
        s3_prefix = f"{output_s3_raw_path}/{transformed_key}/{schema_key}"

    # 4. S3 경로 존재 여부 확인
    if s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
        logger.info(f"S3 경로가 이미 존재하므로 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
        return

    try:
        # 5. Spark DataFrame을 S3에 직접 저장
        s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
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

            # s3 적재 데이터 확인
            logger.info(f"{output_s3_raw_path} - s3 적재 데이터 확인:")
            filtered_df.show(5, truncate=False)

            upload_to_s3(filtered_df, weekly_start_date, output_s3_raw_path, aws_access_key, aws_secret_key)
    
    spark.stop()
    logger.info("SparkSession이 성공적으로 종료되었습니다.")
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Spark 데이터 필터링 및 S3 업로드")
    parser.add_argument("--input_local_path", type=str, required=True, help="입력 데이터 Parquet 파일 경로")
    parser.add_argument("--output_s3_raw_path", type=str, required=True, help="S3 raw-data 업로드 폴더명")
    parser.add_argument("--output_s3_processed_path", type=str, required=True, help="S3 processed-data 업로드 폴더명")
    parser.add_argument("--aws_access_key", required=True, help="AWS Access Key")
    parser.add_argument("--aws_secret_key", required=True, help="AWS Secret key")
    parser.add_argument("--start_date", type=str, required=True, help="필터링 시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="필터링 종료 날짜 (YYYY-MM-DD)")
    
    args = parser.parse_args()

    main(args.input_local_path, args.output_s3_raw_path, args.aws_access_key, args.aws_secret_key, args.start_date, args.end_date)
