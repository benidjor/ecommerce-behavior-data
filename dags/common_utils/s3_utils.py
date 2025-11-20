import logging
import boto3
from datetime import datetime, timedelta

from common_utils.date_utils import detect_date_format
from config.s3_config import S3_BUCKET_NAME


logger = logging.getLogger(__name__)
# S3_BUCKET_NAME = "ecommerce-behavior-data"


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