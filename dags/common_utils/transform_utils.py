from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
import logging
import boto3

logger = logging.getLogger(__name__)


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
    logger.info(f"결측치를 최빈값으로 대체합니다.")
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
    logger.info(f"결측치 처리를 시작합니다.")
    impute_params = [
        ("product_id", "category_code"),  # product_id 기준으로 먼저 채우고
        ("category_id", "category_code"),   # 남은 결측치는 category_id 기준으로 채우기
        ("product_id", "brand"),
        ("category_id", "brand")
    ]
    df = multiple_imputes_by_mode(df, impute_params)

    logger.info(f"대체되지 않은 결측치를 unknown으로 대체합니다.")
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