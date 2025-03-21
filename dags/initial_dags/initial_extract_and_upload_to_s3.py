from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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


def filter_by_date(spark, df, start_date, end_date):
    """
    특정 날짜 범위로 데이터를 필터링
    """
    try:
        if df.rdd.isEmpty():
            logger.warning("데이터프레임이 비어 있습니다.")
            return None

        ymd_column = [col for col in df.columns if col.endswith('ymd')]
        for col in ymd_column:
            df = df.withColumn(col, F.to_date(df[col]))

        logger.info(f"필터링 기준 컬럼: {ymd_column[0]}")
        filtered_df = df.filter(df[ymd_column[0]].between(start_date, end_date))
        return filtered_df

    except Exception as e:
        logger.error(f"파일 처리 중 오류 발생: {e}")
        return None


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
        raise ValueError(f"올바른 날짜 형식이 아닙니다: {date_str} (YYYY-MM-DD 또는 YYMMDD만 허용됨)")
    

def generate_s3_details(start_date, output_s3_raw_path, is_raw=True, schema_key=None):
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

    if is_raw:
        s3_prefix = f"{output_s3_raw_path}/{transformed_key}/"
    else:
        s3_prefix = f"{output_s3_raw_path}/{transformed_key}/{schema_key}"
        
    return s3_prefix, end_date


def is_s3_path_exists(bucket_name, prefix, aws_access_key, aws_secret_key):
    """ 
    S3 경로가 존재하는지 확인
    """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return 'Contents' in response  # 객체가 존재하면 True


def get_missing_weeks(weekly_dict, output_s3_raw_path, aws_access_key, aws_secret_key):
    """
    주어진 weekly_dict를 순회하며, 각 주에 대해 S3 경로가 존재하지 않는 경우(즉, missing week)를 반환
    
    return: missing_weeks: 업로드가 필요한 주에 대한 딕셔너리
    """
    missing_weeks = {}
    for date_range in weekly_dict.values():
        weekly_start_date = date_range["weekly_start_date"]
        weekly_end_date = date_range["weekly_end_date"]
        s3_prefix, _ = generate_s3_details(weekly_start_date, output_s3_raw_path)
        
        if is_s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
            logger.info(f"S3 경로가 이미 존재하므로 해당 주({weekly_start_date} ~ {weekly_end_date}) 데이터 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
        else:
            missing_weeks[weekly_start_date] = {
                "weekly_start_date": weekly_start_date,
                "weekly_end_date": weekly_end_date
            }
    return missing_weeks


def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key, is_raw=True, schema_key=None):
    """
    Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
    해당 경로가 존재하면 저장 스킵
    """
    s3_prefix, end_date = generate_s3_details(start_date, output_s3_raw_path, is_raw, schema_key)

    # 1. S3 경로 존재 여부 확인
    if is_s3_path_exists(S3_BUCKET_NAME, s3_prefix, aws_access_key, aws_secret_key):
        logger.info(f"S3 경로가 이미 존재하므로 업로드를 건너뜁니다: s3://{S3_BUCKET_NAME}/{s3_prefix}")
        return

    try:
        # 2. Spark DataFrame을 S3에 직접 저장
        s3_path = f"s3a://{S3_BUCKET_NAME}/{s3_prefix}"
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
        df.write.mode("overwrite").parquet(s3_path)
        
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")
    
    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)


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
            weekly_filtered_df.orderBy("event_time", "user_id").show(3, truncate=False)
            
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
