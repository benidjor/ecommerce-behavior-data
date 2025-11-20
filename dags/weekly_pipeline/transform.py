import logging
import sys
import argparse
import boto3
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from config.s3_config import S3_BUCKET_NAME, RAW_FOLDER
from config.data_filtering_config import NEW_COLUMN_ORDER

from common_utils.spark_utils import get_spark_session
from common_utils.s3_utils import upload_to_s3, is_s3_path_exists
from common_utils.transform_utils import list_s3_folders, imputation_process, seperate_and_reorder_cols

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(output_s3_processed_path, aws_access_key, aws_secret_key, start_date, end_date):
    """
    1. S3의 RAW 폴더 내부에 있는 각 날짜별 폴더(예: s3a://.../raw-data/191001) 순회
    2. 각 폴더에 대해, 동일 날짜의 Processed 폴더(예: s3a://.../processed-data/191001)가 이미 존재하면
       Transform 및 Upload를 건너뛰고, 존재하지 않으면 Transform 작업을 수행한 후 Upload
    """
    spark = get_spark_session(aws_access_key, aws_secret_key)
    logger.info(f"데이터 Transform 시작 일자: {start_date} ~ {end_date}")

    s3_folders = list_s3_folders(S3_BUCKET_NAME, RAW_FOLDER, aws_access_key, aws_secret_key)
    
    # logger.info(f"s3_folders: {s3_folders}")

    for parquet_prefix in s3_folders:
        
        # s3에서 날짜 가져오기 e.g. s3/raw-data/191001: 191001
        start_date_of_parquet = parquet_prefix.split("/")[-1]
        logger.info(f"Transform 대상 S3 경로: {parquet_prefix}, Transform 기준 날짜: {start_date_of_parquet}")

        # 해당 날짜의 PROCESSED 데이터 폴더가 이미 존재하는지 확인
        processed_prefix = f"{output_s3_processed_path}/{start_date_of_parquet}/"
        if is_s3_path_exists(S3_BUCKET_NAME, processed_prefix, aws_access_key, aws_secret_key):
            logger.info(f"Processed 데이터 폴더가 이미 존재하므로, Transform을 건너뜁니다: s3://{S3_BUCKET_NAME}/{processed_prefix}")
            continue

        logger.info(f"결측치 처리 및 컬럼 재배치")

        # RAW 데이터를 읽은 후 partition 수 확인 및 로그 출력
        df_raw = spark.read.parquet(parquet_prefix)
        # desired_partitions = 6
        # initial_partitions = df_raw.rdd.getNumPartitions()
        # logger.info(f"RAW 데이터의 초기 파티션 수: {initial_partitions}")

        # # 필요에 따라 repartition을 사용하여 파티션 수 조정 (예시: 6개로 재분배)
        # if initial_partitions < desired_partitions:
        #     df_raw = df_raw.repartition(desired_partitions)
        #     logger.info(f"데이터를 {desired_partitions}개의 파티션으로 재분배(repartition) 하였습니다.")
        
        # event_time 컬럼에 NULL 값이 없는 데이터만 필터링하고, 그 결과를 캐싱
        df_filtered = df_raw.filter(F.col("event_time").isNotNull())
        df_filtered.cache()  # 필터링된 DataFrame만 캐싱하여 메모리 사용 최적화
        logger.info("Filtered DataFrame has been cached.")

        # Transform 작업: 결측치 처리 및 컬럼 분리/재배치 전에 cache 적용
        df_transformed = (
            df_filtered
            .transform(imputation_process) # 결측치 처리
            .transform(lambda df: seperate_and_reorder_cols(df, "category_code", "event_time", NEW_COLUMN_ORDER)) # 컬럼 분리 및 재배치
        )

        # df_transformed.orderBy("event_time", "user_id").show(3, truncate=False)
        # Transform 이후 파티션 수 확인 (최적화 결과 확인 용)
        final_partitions = df_transformed.rdd.getNumPartitions()
        logger.info(f"Transform 후 데이터의 파티션 수: {final_partitions}")

        logger.info(f"{processed_prefix}에 Transform 완료한 데이터셋 적재:")

        upload_to_s3(df_transformed, start_date_of_parquet, output_s3_processed_path, aws_access_key, aws_secret_key)

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

    main(args.output_s3_processed_path, args.aws_access_key, args.aws_secret_key, args.start_date, args.end_date)
