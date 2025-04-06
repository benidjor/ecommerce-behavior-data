import logging
from airflow.decorators import task
from custom_modules.utils.spark_helpers import get_spark_session
from custom_modules.utils.data_filtering_helpers import set_filtering_date, filter_by_date
from custom_modules.utils.s3_helpers import upload_to_s3, get_missing_weeks
from custom_modules.config.s3_config import S3_BUCKET_NAME
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

@task
def run_initial_extract_task(input_local_path: str,
                             output_s3_raw_path: str,
                             aws_access_key: str,
                             aws_secret_key: str,
                             start_date: str,
                             end_date: str) -> str:
    """
    초기 데이터 추출 및 S3 업로드 작업 수행
    """
    # 주 단위 날짜 범위 생성
    weekly_dict = set_filtering_date(weekly_start_date=start_date,
                                     weekly_end_date=end_date,
                                     freq="7D")
    # S3에 업로드되지 않은 주 확인
    missing_weeks = get_missing_weeks(weekly_dict, output_s3_raw_path,
                                      aws_access_key, aws_secret_key,
                                      S3_BUCKET_NAME)
    if not missing_weeks:
        logger.info("모든 Weekly 데이터가 이미 S3에 존재합니다. 작업을 건너뜁니다.")
        return "No upload needed"
    
    # Spark 세션 생성 및 데이터 로드
    spark = get_spark_session(aws_access_key, aws_secret_key, app_name="Initial Extract Task")
    logger.info(f"{input_local_path} 에서 데이터 로드 시작")
    df = spark.read.parquet(input_local_path)
    # 전체 데이터 날짜 필터링
    filtered_df = df.filter((F.col("event_time_ymd") >= start_date) &
                              (F.col("event_time_ymd") <= end_date))
    
    # 주차별로 데이터 필터링 후 S3 업로드
    for week_info in missing_weeks.values():
        weekly_start_date = week_info["weekly_start_date"]
        weekly_end_date = week_info["weekly_end_date"]
        weekly_filtered_df = filter_by_date(spark, filtered_df, weekly_start_date, weekly_end_date)
        if not weekly_filtered_df:
            logger.info(f"{weekly_start_date} ~ {weekly_end_date} 데이터가 없어 건너뜁니다.")
            continue
        upload_to_s3(weekly_filtered_df, weekly_start_date, output_s3_raw_path,
                     aws_access_key, aws_secret_key, S3_BUCKET_NAME)
    
    spark.stop()
    logger.info("Spark 세션 종료")
    return "Initial Extract Task Completed"
