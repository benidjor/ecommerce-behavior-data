from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
import pandas as pd

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
        .master("spark://spark-master:7077") \
        .appName("Extract & Transform Data") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.executor.instances", "3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # SparkSession Time Zone을 UTC로 설정
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    logger.info(f"Current TimeZone: {spark.conf.get('spark.sql.session.timeZone')}")

    return spark


def list_s3_folders(bucket_name, prefix, aws_access_key, aws_secret_key):
    """
    boto3 paginator를 사용하여 S3 경로 내 모든 폴더(첫 번째 레벨) 목록을 조회.
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        folder_paths = set()
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                parts = key.split("/")
                # 첫 번째 레벨 폴더 추출: prefix 다음에 오는 폴더명
                if len(parts) > 1 and parts[1]:
                    folder_paths.add(parts[1])
        
        if not folder_paths:
            logger.warning(f"{prefix} 경로에 parquet 폴더가 없습니다.")
            return []
        
        s3_folders = [f"s3a://{bucket_name}/{prefix}/{folder}" for folder in folder_paths]
        logger.info(f"{prefix} 경로에서 {len(s3_folders)}개의 폴더 발견: {s3_folders}")
        return s3_folders

    except Exception as e:
        logger.error(f"S3에서 parquet 폴더 목록을 가져오는 중 오류 발생: {str(e)}", exc_info=True)
        return []
    

def impute_by_mode_value(df: DataFrame, group_col: str, target_col: str) -> DataFrame:
    """
    특정 그룹(group_col) 내에서 target_col의 최빈값(mode)을 찾아 NULL 값을 대체하는 함수.
    리소스 사용 최적화를 위해 필요한 컬럼만 선택하고, 계산된 최빈값 결과를 브로드캐스트 조인.
    
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
    value_counts.cache()  # 중간 결과 캐싱으로 반복 연산 최소화
    
    # 4. Window 함수를 이용해 그룹별로 count 내림차순 정렬 후 최빈값 선택
    window_spec = Window.partitionBy(group_col).orderBy(F.col("count").desc(), F.col(target_col))
    
    mode_df = (
        value_counts
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .select(group_col, F.col(target_col).alias(f"most_frequent_{target_col}"))
    )
    
    # # 5. mode_df를 브로드캐스트하여 조인 성능 향상
    # mode_df = F.broadcast(mode_df)
    
    # # 6. 원본 데이터와 조인한 후, target_col이 NULL인 경우에만 최빈값으로 대체
    # df_imputed = (
    #     df
    #     .join(mode_df, on=group_col, how="left")
    #     .withColumn(target_col, F.coalesce(F.col(target_col), F.col(f"most_frequent_{target_col}")))
    #     .drop(f"most_frequent_{target_col}")
    # )
    
    # 5. 브로드캐스트 조인 적용 (인라인 방식)
    df_imputed = df.join(F.broadcast(mode_df), on=group_col, how="left") \
                   .withColumn(target_col, F.coalesce(F.col(target_col), F.col(f"most_frequent_{target_col}"))) \
                   .drop(f"most_frequent_{target_col}")

    return df_imputed


def multiple_imputes_by_mode(df, impute_params):
    """
    impute_params: 리스트 형식으로 [(group_col, target_col), ...] 전달.
    각 튜플에 대해 impute_by_mode_value를 호출하여 결측치를 채움.
    """
    for group_col, target_col in impute_params:
        df = impute_by_mode_value(df, group_col, target_col)
    return df


def imputation_process(df):
    """
    product_id 기준 → category_id 기준 순으로 category_code, brand 결측치르 채운 후, 
    남은 결측치는 "unknown"으로 처리.
    """
    impute_params = [
        ("product_id", "category_code"),  # product_id 기준으로 먼저 채우고
        ("category_id", "category_code"),   # 남은 결측치는 category_id 기준으로 채우기
        ("product_id", "brand"),
        ("category_id", "brand")
    ]
    df = multiple_imputes_by_mode(df, impute_params)

    # 대체되지 않은 결측치 unknown으로 변경
    total_imputed_df = df.na.fill("unknown")

    return total_imputed_df


def seperate_category_code(df, category_code="category_code"):
    """
    category_code 컬럼을 "." 단위로 분리하여, 
    대분류(category_lv_1), 중분류(category_lv_2), 소분류(category_lv_3)로 나누는 함수입니다.
    만약 category_code가 "unknown"인 경우, 모든 레벨을 "unknown"으로 처리합니다.
    """
    split_col = F.split(F.col(category_code), "\\.")
    levels = [("category_lv_1", None), ("category_lv_2", "none"), ("category_lv_3", "none")]
    
    for i, (col_name, default_val) in enumerate(levels):
        # category_code가 "unknown"이면 무조건 "unknown"으로 처리
        # 그렇지 않은 경우, 해당 인덱스의 값이 있으면 사용하고, 없으면(default_val이 설정되어 있으면) default_val 사용
        df = df.withColumn(
            col_name,
            F.when(F.col(category_code) == "unknown", F.lit("unknown"))
             .otherwise(
                 F.coalesce(split_col.getItem(i), F.lit(default_val)) if default_val is not None 
                 else split_col.getItem(i)
             )
        )
    return df


def separate_event_time_col(df, event_col="event_time"):
    """
    event_time 컬럼 기반 월, 일, 시, 요일 파생 컬럼 생성
    """
   
    # event_time을 기반으로 파생 컬럼을 생성
    transformations = {
        "event_time_month": "MM",       # 월
        "event_time_day": "dd",         # 일
        "event_time_hour": "HH",        # 시
        "event_time_day_name": "E",     # 요일
        # "event_time_ymd": "yyyy-MM-dd",   # 연-월-일
        # "event_time_hms": "HH:mm:ss"      # 시:분:초
    }

    for col_name, format_str in transformations.items():
        df = df.withColumn(col_name, F.date_format(event_col, format_str))

    return df


def seperate_and_reorder_cols(df, category_code, event_col, new_column_order):

    # category_code 컬럼 분리
    df = seperate_category_code(df, category_code)

    # event_time 컬럼 분리
    df = separate_event_time_col(df, event_col)

    # 컬럼 순서 재배치
    df = df.select([F.col(c) for c in new_column_order])
    logger.info(f"DataFrame 컬럼 재배치 완료: {df.columns}")

    return df


def create_star_schema(df):
    """
    입력 DataFrame에서 star schema에 해당하는 Dimension 및 Fact 테이블을 생성.
    
    대규모 데이터셋 환경에서 성능 최적화를 위해 window 함수 대신 monotonically_increasing_id()를 사용하며,
    입력 DataFrame을 캐싱하고, 작은 Dimension 테이블(Time Dimension)에 대해서는 broadcast join을 활용.
    
    Parameters:
      df (DataFrame): DataFrame

    Returns:
      dict: {
         "time_dim": Time Dimension DataFrame,
         "category_dim": Category Dimension DataFrame,
         "product_dim": Product Dimension DataFrame,
         "user_dim": User Dimension DataFrame,
         "fact_df": Fact 테이블 DataFrame
      }
    """
    
    # 캐시를 통해 여러 번의 재계산 방지
    df = df.cache()
    
    # 각 Dimension에 사용할 컬럼 목록 변수화
    time_cols = ["event_time", "event_time_ymd", "event_time_hms", 
                 "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name"]
    category_cols = ["category_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3"]
    product_cols = ["product_id", "brand", "price"]
    user_cols = ["user_id", "user_session"]
    
    # 1. Time Dimension 생성 (window 함수 대신 monotonically_increasing_id() 사용)
    # dropDuplicates() 후, 각 고유 row에 대해 고유한 surrogate key를 부여
    time_dim = df.select(*time_cols).dropDuplicates()
    time_dim = time_dim.withColumn("time_id", F.monotonically_increasing_id()) \
                       .select("time_id", *time_cols)
    
    # Time Dimension은 dropDuplicates 후 일반적으로 크기가 작으므로 broadcast join 적용
    time_dim_broadcasted = F.broadcast(time_dim)
    
    # 2. Category Dimension 생성
    category_dim = df.select(*category_cols).dropDuplicates()
    
    # 3. Product Dimension 생성
    product_dim = df.select(*product_cols).dropDuplicates()
    
    # 4. User Dimension 생성
    user_dim = df.select(*user_cols).dropDuplicates()
    
    # 5. Fact 테이블 생성: Time Dimension과의 join 시 broadcast 처리된 time_dim 사용
    fact_df = df.join(time_dim_broadcasted, on=time_cols, how="left")
    fact_df = fact_df.select("user_id", "category_id", "product_id", "event_type", "time_id")
    
    # event_id는 fact 테이블 각 행에 대해 고유하게 부여
    fact_df = fact_df.withColumn("event_id", F.monotonically_increasing_id()) \
                     .select("event_id", "time_id", "category_id", "product_id", "user_id", "event_type")
        
    star_schema_dict = {
        "time_dim": time_dim,
        "category_dim": category_dim,
        "product_dim": product_dim,
        "user_dim": user_dim,
        "fact_df": fact_df
    }

    df.unpersist() # 캐시 해제
    
    return star_schema_dict


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


def generate_s3_details(start_date, output_s3_raw_path):
    """
    주어진 start_date와 output_s3_raw_path를 기반으로 S3 prefix와 종료 날짜(end_date)를 생성
    
    - start_date: 시작 날짜 (문자열 혹은 datetime)
    - output_s3_raw_path: S3의 기본 경로
    - is_raw: raw 데이터 여부 (True이면 schema_key 미포함)
    - schema_key: raw가 아닌 경우에 추가할 schema key
    
    return:
      s3_prefix: S3 저장 경로 (예: "{output_s3_raw_path}/YYMMDD/" 또는 "{output_s3_raw_path}/YYMMDD/{schema_key}")
      end_date: start_date 기준 6일 후 날짜 (문자열, "YYYY-MM-DD" 형식)
    """
    # 1. start_date의 형식 자동 감지 및 datetime 변환
    date_obj = detect_date_format(start_date)

    # 2. 종료 날짜 계산
    end_date = (date_obj + timedelta(days=6)).strftime("%Y-%m-%d")

    # 3. S3 저장 경로 설정 (YYMMDD 형식)
    transformed_key = date_obj.strftime("%y%m%d")  # YYMMDD 형식으로 변환

    s3_prefix = f"{output_s3_raw_path}/{transformed_key}/"
        
    return s3_prefix, end_date


def is_s3_path_exists(bucket_name, prefix, aws_access_key, aws_secret_key):
    """ 
    S3 경로가 존재하는지 확인
    """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return 'Contents' in response  # 객체가 존재하면 True


def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key, is_raw=True, schema_key=None):
    """
    Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
    해당 경로가 존재하면 저장을 건너뜀
    """
    s3_prefix, end_date = generate_s3_details(start_date, output_s3_raw_path)

    # 4. S3 경로 존재 여부 확인
    if is_s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
        logger.info(f"S3 경로가 이미 존재하므로 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
        return

    try:
        # 5. Spark DataFrame을 S3에 직접 저장
        s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
        df.write.mode("overwrite").option("compression", "snappy").parquet(s3_path)
        
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")
    
    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)


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
        desired_partitions = 6
        initial_partitions = df_raw.rdd.getNumPartitions()
        logger.info(f"RAW 데이터의 초기 파티션 수: {initial_partitions}")

        # 필요에 따라 repartition을 사용하여 파티션 수 조정 (예시: 6개로 재분배)
        if initial_partitions < desired_partitions:
            df_raw = df_raw.repartition(desired_partitions)
            logger.info(f"데이터를 {desired_partitions}개의 파티션으로 재분배(repartition) 하였습니다.")
        
        # event_time 컬럼에 NULL 값이 없는 데이터만 필터링하고, 그 결과를 캐싱
        df_filtered = df_raw.filter(F.col("event_time").isNotNull())
        df_filtered.cache()  # 필터링된 DataFrame만 캐싱하여 메모리 사용 최적화
        logger.info("Filtered DataFrame has been cached.")

        # Transform 작업: 결측치 처리 및 컬럼 분리/재배치 전에 cache 적용
        df_transformed = (
            df_filtered
            .transform(imputation_process) # 결측치 처리
            .transform(lambda df: seperate_and_reorder_cols(df, "category_code", "event_time", new_column_order)) # 컬럼 분리 및 재배치
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
