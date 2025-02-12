from pyspark.sql import SparkSession
# from pyspark.sql.functions import first, col, count, sum, round, countDistinct, lit, coalesce, row_number, to_date, date_format, split, when
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
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

new_column_order = [
    "user_id", "user_session", "category_id", "event_time", "event_time_ymd", "event_time_hms", "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name", "event_type", "product_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3",  "brand", "price"
]

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


def list_s3_folders(bucket_name, prefix, aws_access_key, aws_secret_key):
    """
    S3에서 특정 prefix 경로 아래의 폴더 목록을 가져오기
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            logger.warning(f"{prefix} 경로에 parquet 폴더가 없습니다.")
            return []


        # 폴더 경로 추출
        folder_paths = set()
        for obj in response['Contents']:
            key = obj['Key']
            day_key = key.split("/")[1]
            folder_paths.add(day_key)
            # parts = key[len(prefix):].split("/")  # Prefix 이후의 경로를 분할
            # if len(parts) > 1 and parts[0]:  # 첫 번째 레벨 폴더만 가져옴
            #     folder_paths.add(f"{prefix.rstrip('/')}/{parts[0]}")
        logger.info(f"s3_folders: {folder_paths}")

        s3_folders = [f"s3a://{bucket_name}/{prefix}/{folder}" for folder in folder_paths]
        
        logger.info(f"s3_folders: {s3_folders}")
        logger.info(f"{prefix} 경로에서 {len(s3_folders)}개의 parquet 폴더 발견.")
        return s3_folders

    except Exception as e:
        logger.error(f"S3에서 parquet 폴더 목록을 가져오는 중 오류 발생: {str(e)}")
        return []
    

def seperate_category_code(df, category_code="category_code"):
    """
    category_code 컬럼을 "." 단위로 나누어, 대분류, 중분류, 소분류로 분리
    """
    # Split 결과를 배열로 저장
    category_split_col = F.split(F.col(category_code), "\\.")

    # 컬럼 추가
    df = df.withColumns(
        {
            "category_lv_1": category_split_col.getItem(0),
            "category_lv_2": F.when(category_split_col.getItem(1).isNotNull(), category_split_col.getItem(1)).otherwise("None"),
            "category_lv_3": F.when(category_split_col.getItem(2).isNotNull(), category_split_col.getItem(2)).otherwise("None")
        }
    )

    return df


def separate_event_time_col(df, event_col="event_time"):
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
        df = df.withColumn(col_name, F.date_format(event_col, format_str))

    return df


def impute_by_mode_value(df: DataFrame, group_col: str, target_col: str) -> DataFrame:
    """
    특정 그룹(group_col) 내에서 target_col의 최빈값(mode)을 찾아 NULL 값을 대체하는 함수입니다.
    이 함수는 리소스 사용 최적화를 위해 필요한 컬럼만 선택하고, 계산된 최빈값 결과를 브로드캐스트 조인으로 사용합니다.
    
    Parameters:
    - df: 입력 Spark DataFrame
    - group_col: 그룹화할 컬럼명 (예: category_id)
    - target_col: 최빈값을 찾아 NULL을 채울 컬럼명 (예: category_code)
    
    return: target_col의 NULL 값이 해당 그룹의 non-null 최빈값으로 채워진 DataFrame
    """
    # 1. 필요한 컬럼만 선택 (group_col, target_col)
    df_reduced = df.select(group_col, target_col)
    
    # 2. non-null인 target_col만 고려하여 집계
    non_null_df = df_reduced.filter(F.col(target_col).isNotNull())
    
    # 3. 그룹별로 target_col 값의 count 계산
    value_counts = (
        non_null_df
        .groupBy(group_col, target_col)
        .agg(F.count("*").alias("count"))
    )
    
    # 4. Window 함수를 이용해 그룹별로 count 내림차순 정렬 후 최빈값 선택
    window_spec = Window.partitionBy(group_col).orderBy(F.col("count").desc(), F.col(target_col))
    
    mode_df = (
        value_counts
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .select(group_col, F.col(target_col).alias(f"most_frequent_{target_col}"))
    )
    
    # 5. mode_df를 브로드캐스트하여 조인 성능 향상
    mode_df = F.broadcast(mode_df)
    
    # 6. 원본 데이터와 조인한 후, target_col이 NULL인 경우에만 최빈값으로 대체
    df_imputed = (
        df
        .join(mode_df, on=group_col, how="left")
        .withColumn(target_col, F.coalesce(F.col(target_col), F.col(f"most_frequent_{target_col}")))
        .drop(f"most_frequent_{target_col}")
    )
    
    return df_imputed


def drop_null_and_reorder_cols(df, prefix, new_column_order):
    """
    결측치를 제거하고 Spark DataFrame의 컬럼 순서 변경
    """
    # 결측치 처리
    before_drop = df.count()
    df = df.na.drop()
    after_drop = df.count()
    dropped_rows = before_drop - after_drop
    logger.info(f"{prefix}에서 {before_drop}개 행 중 {dropped_rows}개의 결측치 행 제거.")

    # 입력된 컬럼 리스트가 실제 컬럼과 일치하는지 확인
    if set(new_column_order) != set(df.columns):
        raise ValueError("입력한 컬럼 리스트가 DataFrame의 컬럼과 일치하지 않습니다.")

    df = df.select([F.col(c) for c in new_column_order])

    return df


def detect_date_format(date_str):
    """ 
    주어진 날짜가 'YYYY-MM-DD'인지 'YYMMDD'인지 판별 후 datetime 객체로 변환 
    """
    try:
        # YYYY-MM-DD 형식일 경우
        return datetime.strptime(date_str, "%Y-%m-%d")
    
    except ValueError:
        pass  # 다음 형식 검사

    try:
        # YYMMDD 형식일 경우
        return datetime.strptime(date_str, "%y%m%d")
    
    except ValueError:
        raise ValueError(f"올바른 날짜 형식이 아닙니다: {date_str} (YYYY-MM-DD 또는 YYMMDD만 허용)")


def s3_path_exists(bucket_name, prefix, aws_access_key, aws_secret_key):
    """ 
    S3 경로가 존재하는지 확인
    """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    return 'Contents' in response  # 객체가 존재하면 True


def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key):
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

    s3_prefix = f"{output_s3_raw_path}/{transformed_key}/"

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


def main(output_s3_processed_path, aws_access_key, aws_secret_key, start_date, end_date):
    spark = get_spark_session(aws_access_key, aws_secret_key)
    logger.info(f"데이터 Transform 시작 일자: {start_date} ~ {end_date}")

    s3_folders = list_s3_folders(S3_BUCKET_NAME, RAW_FOLDER, aws_access_key, aws_secret_key)
    
    logger.info(f"s3_folders: {s3_folders}")

    for parquet_prefix in s3_folders:
        
        # s3에서 날짜 가져오기 e.g. s3/raw-data/191001: 191001
        start_date_of_parquet = parquet_prefix.split("/")[-1]
        logger.info(f"Transform 대상 S3 경로: {parquet_prefix}, Transform 기준 날짜: {start_date_of_parquet}")

        # parquet 데이터 읽기
        weekly_raw_df = spark.read.parquet(parquet_prefix)

        # category_code 컬럼 분리
        weekly_cat_code_seperated_df = seperate_category_code(weekly_raw_df)

        # event_time 컬럼 분리
        weekly_event_time_seperated_df = separate_event_time_col(weekly_cat_code_seperated_df)

        # 결측치 처리: category_id를 기준으로 category_code 결측치 대체
        imputed_cat_df = impute_by_mode_value(weekly_event_time_seperated_df, "category_id", "category_code")

        # 결측치 처리: product_id를 기준으로 category_code 결측치 대체
        imputed_cat_df = impute_by_mode_value(imputed_cat_df, "product_id", "category_code")

        # 결측치 처리: product_id를 기준으로 brand 결측치 대체
        imputed_cat_br_df = impute_by_mode_value(imputed_cat_df, "product_id", "brand")

        # 결측치 처리: category_id를 기준으로 brand 결측치 대체
        imputed_cat_br_df = impute_by_mode_value(imputed_cat_br_df, "category_id", "brand")

        # 대체되지 않은 결측치 제거 및 데이터프레임 컬럼 재배치
        processed_df = drop_null_and_reorder_cols(imputed_cat_br_df, parquet_prefix, new_column_order)

        logger.info(f"Transform 완료된 DataFrame:")
        processed_df.show(5)

        upload_to_s3(processed_df, start_date_of_parquet, output_s3_processed_path, aws_access_key, aws_secret_key)


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
