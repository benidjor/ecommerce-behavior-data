import logging
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from config.s3_config import S3_BUCKET_NAME

from common_utils.spark_utils import get_spark_session
from common_utils.date_utils import set_filtering_date, filter_by_date
from common_utils.s3_utils import get_missing_weeks, generate_s3_details

logger = logging.getLogger(__name__)

# S3_BUCKET_NAME = "ecommerce-behavior-data"
# RAW_FOLDER = "raw-data"


def main(input_local_path, output_s3_raw_path, aws_access_key, aws_secret_key, start_date, end_date):
    """
    1. weekly_dict를 생성한 후, 각 주의 S3 경로 존재 여부를 별도의 함수(get_missing_weeks)로 확인
    2. 업로드가 필요한 주(missing weeks)가 없다면 전체 작업을 스킵
    3. 업로드가 필요한 주에 대해서만 전체 데이터를 로드한 후 필터링 및 S3 업로드 진행
    """
    # 주 단위 날짜 범위 생성
    weekly_dict = set_filtering_date(weekly_start_date=start_date, weekly_end_date=end_date, freq="7D")
    
    # 업로드가 필요한 주(즉, S3 폴더에 없는 주) 목록을 가져오기
    missing_weeks = get_missing_weeks(weekly_dict, output_s3_raw_path, aws_access_key, aws_secret_key)
    
    if not missing_weeks:
        logger.info("모든 Weekly 데이터가 이미 S3에 존재합니다. 전체 작업을 건너뜁니다.")
        return

    # SparkSession 시작 및 전체 데이터 로드
    spark = get_spark_session(aws_access_key, aws_secret_key)

    # 전체 데이터를 한 번 로드하여 캐싱
    logger.info(f"{input_local_path} - 로컬 디렉토리 데이터셋 불러오는 중")
    df = spark.read.parquet(input_local_path)
    filtered_df = df.filter((F.col("event_time_ymd") >= start_date) & (F.col("event_time_ymd") <= end_date))

    # df.cache()
    # df.count()  # 캐시가 실제 메모리에 로드되도록 강제 액션 실행

    # 업로드가 필요한 주에 대해서만 필터링 및 S3 업로드
    for week_info in missing_weeks.values():
        missing_weekly_start_date = week_info["weekly_start_date"]
        missing_weekly_end_date = week_info["weekly_end_date"]
        
        weekly_filtered_df = filter_by_date(spark, filtered_df, missing_weekly_start_date, missing_weekly_end_date)
        if not weekly_filtered_df:
            logger.info(f"주차 {missing_weekly_start_date} ~ {missing_weekly_end_date}에 해당하는 데이터가 없어 스킵합니다.")
            continue
        
        s3_prefix, computed_end_date = generate_s3_details(missing_weekly_start_date, output_s3_raw_path)
        try:
            s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
            logger.info(f"일주일 ({missing_weekly_start_date} ~ {missing_weekly_end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
            # weekly_filtered_df.orderBy("event_time", "user_id").show(3, truncate=False)
            
            weekly_filtered_df.write.mode("overwrite").parquet(s3_path)
            logger.info(f"일주일 ({missing_weekly_start_date} ~ {missing_weekly_end_date}) 데이터 업로드 완료.")

        except Exception as e:
            logger.error(f"일주일 ({missing_weekly_start_date} ~ {missing_weekly_end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)
    
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
