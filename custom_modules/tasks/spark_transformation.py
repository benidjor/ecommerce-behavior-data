#!/usr/bin/env python
import argparse
import logging
import boto3
import pyspark.sql.functions as F
from custom_modules.utils.spark_helpers import get_spark_session
from custom_modules.utils.s3_helpers import upload_to_s3, is_s3_path_exists
from custom_modules.config.s3_config import S3_BUCKET_NAME, RAW_FOLDER, PROCESSED_FOLDER
from custom_modules.tasks.spark_tasks import imputation_process, seperate_and_reorder_cols

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(args):
    """
    데이터 변환 및 S3 업로드 작업 수행 함수
    1. S3의 RAW 폴더 내부에 있는 각 날짜별 폴더(예: s3a://.../raw-data/191001) 순회
    2. 각 폴더에 대해, 동일 날짜의 Processed 폴더(예: s3a://.../processed-data/191001)가 이미 존재하면
       Transform 및 Upload를 건너뛰고, 존재하지 않으면 Transform 작업을 수행한 후 Upload
    """
    # 명령행 인자 값 읽기
    input_raw_s3_path = args.input_raw_s3_path
    output_s3_processed_path = args.output_s3_processed_path
    aws_access_key = args.aws_access_key
    aws_secret_key = args.aws_secret_key
    start_date = args.start_date
    end_date = args.end_date

    # Spark 세션 생성 (예: 동적 할당 등은 get_spark_session 내부에서 처리)
    spark = get_spark_session(
        aws_access_key, 
        aws_secret_key, 
        master="spark://spark-master:7077", 
        app_name="Spark Transformation Task"
    )

    logger.info(f"Transform 작업 시작: {start_date} ~ {end_date}")

    # boto3를 사용하여 S3 RAW 폴더 내 날짜별 폴더 목록 수집
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_FOLDER)
    parquet_folders = set()
    for obj in response.get("Contents", []):
        key = obj["Key"]
        parts = key.split("/")
        if len(parts) > 1 and parts[1]:
            parquet_folders.add(parts[1])
    s3_folders = [f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/{folder}" for folder in parquet_folders]

    # 각 날짜별 폴더에 대해 변환 작업 수행
    for parquet_prefix in s3_folders:
        # 예: s3a://.../raw-data/191001 → 기준 날짜: 191001
        start_date_of_parquet = parquet_prefix.split("/")[-1]
        logger.info(f"Transform 대상 폴더: {parquet_prefix}, 기준 날짜: {start_date_of_parquet}")
        processed_prefix = f"{PROCESSED_FOLDER}/{start_date_of_parquet}/"

        # 해당 날짜의 PROCESSED 데이터 폴더가 이미 존재하는지 확인
        if is_s3_path_exists(S3_BUCKET_NAME, processed_prefix, aws_access_key, aws_secret_key):
            logger.info(f"Processed 데이터 이미 존재: s3://{S3_BUCKET_NAME}/{processed_prefix}")
            continue

        logger.info("결측치 처리 및 컬럼 재배치")
        df_raw = spark.read.parquet(parquet_prefix)
        initial_partitions = df_raw.rdd.getNumPartitions()
        logger.info(f"초기 파티션 수: {initial_partitions}")

        # 파티션 수가 부족하면 필요에 따라 repartition을 사용하여 파티션 수 조정 (예시: 6개로 재분배)
        desired_partitions = 6
        if initial_partitions < desired_partitions:
            df_raw = df_raw.repartition(desired_partitions)
            logger.info(f"데이터를 {desired_partitions} 파티션으로 재분배")

        new_column_order = [
            "user_id", "user_session", "category_id", "event_time", "event_time_ymd", "event_time_hms",
            "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name", "event_type",
            "product_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3", "brand", "price"
        ]

        logger.info(f"Tramsform 작업 시작")

        # DataFrame 변환: 결측치 처리, 컬럼 분리 및 재배치
        df_transformed = (
            df_raw
            .cache()
            .transform(imputation_process)
            .transform(lambda df: seperate_and_reorder_cols(df, "category_code", "event_time", new_column_order))
        )
        final_partitions = df_transformed.rdd.getNumPartitions()
        logger.info(f"Transform 후 파티션 수: {final_partitions}")
        logger.info(f"{processed_prefix}에 Transform 완료한 데이터셋 적재:")
        
        upload_to_s3(df_transformed, start_date_of_parquet, PROCESSED_FOLDER, aws_access_key, aws_secret_key, S3_BUCKET_NAME, is_raw=False)

    spark.stop()
    logger.info("SparkSession이 종료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Transformation Task")
    parser.add_argument("--input_raw_s3_path", type=str, required=True, help="입력 S3 RAW 데이터 폴더 경로")
    parser.add_argument("--output_s3_processed_path", type=str, required=True, help="S3 processed-data 업로드 폴더명")
    parser.add_argument("--aws_access_key", required=True, help="AWS Access Key")
    parser.add_argument("--aws_secret_key", required=True, help="AWS Secret Key")
    parser.add_argument("--start_date", type=str, required=True, help="필터링 시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="필터링 종료 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()
    main(args)
