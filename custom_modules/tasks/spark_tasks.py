# plugins/tasks/spark_tasks.py
import argparse
import logging
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import boto3
from airflow.decorators import task
from custom_modules.utils.spark_helpers import get_spark_session
from custom_modules.utils.s3_helpers import upload_to_s3, is_s3_path_exists
from custom_modules.config.s3_config import S3_BUCKET_NAME, RAW_FOLDER


logger = logging.getLogger(__name__)

# === 데이터 변환 함수들 ===
def impute_by_mode_value(df, group_col, target_col):
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
        ("product_id", "category_code"),    # product_id 기준으로 먼저 채우고
        ("category_id", "category_code"),   # 남은 결측치는 category_id 기준으로 채우기
        ("product_id", "brand"),
        ("category_id", "brand")
    ]
    df = multiple_imputes_by_mode(df, impute_params)
    # 대체되지 않은 결측치 unknown으로 변경
    df = df.na.fill("unknown")
    return df


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
    time_cols = ["event_time", "event_time_ymd", "event_time_hms", 
                 "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name"]
    category_cols = ["category_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3"]
    product_cols = ["product_id", "brand", "price"]
    user_cols = ["user_id", "user_session"]
    
    # 캐시를 통해 여러 번의 재계산 방지
    df = df.cache()
    
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


# === 메인 작업 함수 ===
@task
def run_spark_transformation_task(input_raw_s3_path: str,
                                  output_s3_processed_path: str,
                                  aws_access_key: str,
                                  aws_secret_key: str,
                                  start_date: str,
                                  end_date: str,
                                  desired_partitions: int = 6) -> str:

    """
    데이터 변환 및 S3 업로드 작업 수행 함수
    1. S3의 RAW 폴더 내부에 있는 각 날짜별 폴더(예: s3a://.../raw-data/191001) 순회
    2. 각 폴더에 대해, 동일 날짜의 Processed 폴더(예: s3a://.../processed-data/191001)가 이미 존재하면
       Transform 및 Upload를 건너뛰고, 존재하지 않으면 Transform 작업을 수행한 후 Upload
    """
    spark_master = "spark://spark-master:7077"
    # extra_conf = {
    #     "spark.executor.memory": "6g",
    #     "spark.executor.cores": "2",
    #     "spark.executor.instances": "2",
    #     "spark.driver.memory": "1g"
    # }
    
    spark = get_spark_session(aws_access_key, 
                              aws_secret_key, 
                              master=spark_master, 
                              app_name="Spark Transformation Task")
                            #   extra_conf=extra_conf)                                            
    
    logger.info(f"Transform 작업 시작: {start_date} ~ {end_date}")
    
    # S3 RAW 폴더 내 날짜별 폴더 목록 얻기 (boto3 사용)
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=RAW_FOLDER)
    parquet_folders = set()

    for obj in response.get("Contents", []):
        key = obj["Key"]
        parts = key.split("/")
        if len(parts) > 1 and parts[1]:
            parquet_folders.add(parts[1])
    s3_folders = [f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/{folder}" for folder in parquet_folders]
    
    for parquet_prefix in s3_folders:
         # s3에서 날짜 가져오기 e.g. s3/raw-data/191001: 191001
        start_date_of_parquet = parquet_prefix.split("/")[-1]
        logger.info(f"Transform 대상 폴더: {parquet_prefix}, 기준 날짜: {start_date_of_parquet}")
        processed_prefix = f"{output_s3_processed_path}/{start_date_of_parquet}/"

        # 해당 날짜의 PROCESSED 데이터 폴더가 이미 존재하는지 확인
        if is_s3_path_exists(S3_BUCKET_NAME, processed_prefix, aws_access_key, aws_secret_key):
            logger.info(f"Processed 데이터 이미 존재: s3://{S3_BUCKET_NAME}/{processed_prefix}")
            continue
        
        logger.info(f"결측치 처리 및 컬럼 재배치")

        # RAW 데이터를 읽은 후 partition 수 확인 및 로그 출력
        df_raw = spark.read.parquet(parquet_prefix)
        initial_partitions = df_raw.rdd.getNumPartitions()
        logger.info(f"초기 파티션 수: {initial_partitions}")

        # 필요에 따라 repartition을 사용하여 파티션 수 조정 (예시: 6개로 재분배)
        if initial_partitions < desired_partitions:
            df_raw = df_raw.repartition(desired_partitions)
            logger.info(f"데이터를 {desired_partitions} 파티션으로 재분배")
        
        new_column_order = [
            "user_id", "user_session", "category_id", "event_time", "event_time_ymd", "event_time_hms",
            "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name", "event_type",
            "product_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3", "brand", "price"
        ]

        logger.info(f"Tramsform 작업 시작")
        # Transform 작업: 결측치 처리 및 컬럼 분리/재배치 전에 cache 적용
        df_transformed = (
            df_raw
            .cache()  # 동일 데이터 재사용 시 재계산을 방지
            .transform(imputation_process) # 결측치 처리
            .transform(lambda df: seperate_and_reorder_cols(df, "category_code", "event_time", new_column_order)) # 컬럼 분리 및 재배치
        )
        # df_transformed.orderBy("event_time", "user_id").show(3, truncate=False)
        
        # Transform 이후 파티션 수 확인 (최적화 결과 확인 용)
        logger.info(f"Transform 이후 파티션 수 확인")
        final_partitions = df_transformed.rdd.getNumPartitions()        
        logger.info(f"Transform 후 파티션 수: {final_partitions}")

        logger.info(f"{processed_prefix}에 Transform 완료한 데이터셋 적재:")
        
        upload_to_s3(df_transformed, start_date_of_parquet, output_s3_processed_path, aws_access_key, aws_secret_key, S3_BUCKET_NAME, is_raw=False)
    
    spark.stop()
    logger.info("SparkSession이 성공적으로 종료되었습니다.")
