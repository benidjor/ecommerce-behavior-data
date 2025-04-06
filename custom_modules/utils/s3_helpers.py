from datetime import datetime, timedelta
import boto3
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def detect_date_format(date_str):
    """
    주어진 날짜 문자열을 datetime 객체로 변환 (YYYY-MM-DD 또는 YYMMDD 형식 지원)
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        pass
    try:
        return datetime.strptime(date_str, "%y%m%d")
    except ValueError:
        raise ValueError(f"올바른 날짜 형식이 아닙니다: {date_str} (YYYY-MM-DD 또는 YYMMDD만 허용)")

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


def get_missing_weeks(weekly_dict, output_s3_raw_path, aws_access_key, aws_secret_key, bucket_name):
    """
    주어진 weekly_dict를 순회하며, 각 주에 대해 S3 경로가 존재하지 않는 경우(즉, missing week)를 반환
    
    return: missing_weeks: 업로드가 필요한 주에 대한 딕셔너리
    """
    missing_weeks = {}
    for date_range in weekly_dict.values():
        weekly_start_date = date_range["weekly_start_date"]
        weekly_end_date = date_range["weekly_end_date"]
        s3_prefix, _ = generate_s3_details(weekly_start_date, output_s3_raw_path)
        if is_s3_path_exists(bucket_name, s3_prefix, aws_access_key, aws_secret_key):
            logger.info(f"S3 경로가 존재하여 스킵: s3://{bucket_name}/{s3_prefix}")
        else:
            missing_weeks[weekly_start_date] = {"weekly_start_date": weekly_start_date, "weekly_end_date": weekly_end_date}
    return missing_weeks


def upload_to_s3(df, start_date, output_s3_raw_path, aws_access_key, aws_secret_key, bucket_name, is_raw=True, schema_key=None):
    """
    Spark DataFrame을 S3에 Parquet 형식으로 저장하되,
    해당 경로가 존재하면 저장 스킵
    """
    s3_prefix, end_date = generate_s3_details(start_date, output_s3_raw_path, is_raw, schema_key)

    # 1. S3 경로 존재 여부 확인
    if is_s3_path_exists(bucket_name, s3_prefix, aws_access_key, aws_secret_key):
        logger.info(f"S3 경로 존재함, 업로드 스킵: s3://{bucket_name}/{s3_prefix}")
        return

    try:
        # 2. Spark DataFrame을 S3에 직접 저장
        s3_path = f"s3a://{bucket_name}/{s3_prefix}"
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 저장합니다 ({s3_path})")
        
        df.write.mode("overwrite").parquet(s3_path)
        
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 완료.")
    
    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}", exc_info=True)