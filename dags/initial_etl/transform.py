from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, count, sum, round, countDistinct, lit, coalesce, row_number, to_date, date_format, split, when
from pyspark.sql.window import Window
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
    category_split_col = split(col(category_code), ".")

    # 컬럼 추가
    df = df.withColumns(
        {
            "category_lv_1": category_split_col.getItem(0),
            "category_lv_2": when(category_split_col.getItem(1).isNotNull(), category_split_col.getItem(1)).otherwise("None"),
            "category_lv_3": when(category_split_col.getItem(2).isNotNull(), category_split_col.getItem(2)).otherwise("None")
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
        df = df.withColumn(col_name, date_format(event_col, format_str))

    return df

def impute_by_mode_value(df, group_col, target_col):
    """
    특정 그룹(group_col) 내에서 target_col의 최빈값을 찾아 NULL 값을 대체하는 함수.

    :param df: 입력 Spark DataFrame
    :param group_col: 그룹화할 컬럼명 (예: category_id)
    :param target_col: 최빈값을 찾고 NULL을 채울 컬럼명 (예: category_code)
    :return: NULL 값이 최빈값으로 채워진 DataFrame
    """

    # 1️. group_col별 target_col에서 최빈값 찾기
    value_counts = (
        df.groupBy(group_col, target_col)
        .agg(count("*").alias("count"))  # count 횟수 계산
    )

    # 2️. 최빈값을 찾기 위한 Window 함수 설정
    window_spec = Window.partitionBy(group_col).orderBy(col("count").desc(), col(target_col))

    most_frequent_values = (
        value_counts
        .withColumn("rank", row_number().over(window_spec))  # 최빈값 순위 매기기
        .filter(col("rank") == 1)  # 최빈값 선택
        .select(col(group_col), col(target_col).alias(f"most_frequent_{target_col}"))
    )

    # 3️. 기존 데이터와 조인 후 NULL 값 채우기
    df = (
        df
        .join(most_frequent_values, on=group_col, how="left")  # group_col 기준으로 병합
        .withColumn(target_col, coalesce(col(target_col), col(f"most_frequent_{target_col}")))
        .drop(f"most_frequent_{target_col}") 
    )

    return df

def drop_null(df, parquet):
    # 결측치 처리
    before_drop = df.count()
    df = df.na.drop()
    after_drop = df.count()
    dropped_rows = before_drop - after_drop
    logger.info(f"{parquet}에서 {before_drop}개 행 중 {dropped_rows}개의 결측치 행 제거.")

    return df

# def upload_to_s3(df, start_date, output_s3_processed_path):
#     """
#     Spark DataFrame을 직접 S3에 Parquet 형식으로 저장하는 함수
#     """
#     # 1️. 종료 날짜 계산
#     end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

#     # 2️. S3 저장 경로 설정 (YYMMDD 형식)
#     transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
#     s3_path = f"s3a://{S3_BUCKET_NAME}/{output_s3_processed_path}/{transformed_key}/"

#     try:
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
#         # 3️. Spark DataFrame을 S3에 직접 저장
#         df.write.mode("overwrite").parquet(s3_path)
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")

#     except Exception as e:
#         logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)

# def s3_path_exists(bucket_name, prefix, aws_access_key, aws_secret_key):
#     """ S3 경로가 존재하는지 확인하는 함수 """
#     s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    
#     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
#     return 'Contents' in response  # 객체가 존재하면 True

# def upload_to_s3(df, start_date, output_s3_processed_path, aws_access_key, aws_secret_key):
#     """
#     Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
#     해당 경로가 존재하면 저장을 건너뛴다.
#     """
#     # 1️. 종료 날짜 계산
#     end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

#     # 2️. S3 저장 경로 설정 (YYMMDD 형식)
#     transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
#     s3_prefix = f"{output_s3_processed_path}/{transformed_key}/"

#     # 3. S3 경로 존재 여부 확인
#     if s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
#         logger.info(f"S3 경로가 이미 존재하므로 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
#         return

#     try:
#         # 4. Spark DataFrame을 S3에 직접 저장
#         s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
#         df.write.mode("overwrite").parquet(s3_path)
        
#         logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")
    
#     except Exception as e:
#         logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)

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

def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key):
    """
    Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
    해당 경로가 존재하면 저장을 건너뛴다.
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

    for parquet_folder in s3_folders:

        start_date_of_parquet = parquet_folder.split("/")[-1]
        logger.info(f"Transform 대상 S3 경로: {parquet_folder}, Transform 기준 날짜: {start_date_of_parquet}")

        weekly_raw_df = spark.read.parquet(parquet_folder)

        weekly_raw_df.show(5)

        weekly_cat_code_seperated_df = seperate_category_code(weekly_raw_df)

        weekly_event_time_seperated_df = separate_event_time_col(weekly_cat_code_seperated_df)

        imputed_cat_df = impute_by_mode_value(weekly_event_time_seperated_df, "category_id", "category_code")

        imputed_cat_br_df = impute_by_mode_value(imputed_cat_df, "category_id", "category_code")

        processed_df = drop_null(imputed_cat_br_df, parquet_folder)

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
