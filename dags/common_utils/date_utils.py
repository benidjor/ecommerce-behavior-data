import pandas as pd
from datetime import datetime, timedelta
from typing import Union
import logging
from airflow.models import Variable
from pyspark.sql import functions as F

# 로깅 설정
logger = logging.getLogger(__name__)

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
    

def reset_variable(context, **kwargs):
    run_state = context["run_state"]
    dag_run = kwargs.get("dag_run")
    if dag_run:
        logger.info(f"DAG run_type: {dag_run.run_type}, RUN_STATE_FLAG: {run_state}")

     # 최초 실행(pending) 또는 강제 초기화(force_reset)인 경우 초기화 수행
    if run_state in ["pending", "force_reset"]:
        logger.info("Variable 및 S3 데이터 초기화를 수행합니다.")

        # Variable 초기화
        Variable.set("current_date", "2019-12-03")
        logger.info("Variable 초기화가 완료되었습니다.")
    else:
        logger.info("초기화 작업을 건너뜁니다. DAG가 이전에 이미 실행되었습니다.")


def get_task_context(RUN_STATE_FLAG, CURRENT_DATE, START_DATE):
    """
    DAG 실행을 위한 컨텍스트를 반환합니다.
    Airflow에서 컨텍스트: 태스크 실행과 관련된 정보의 집합 (DAG 실행 정보, 환경 변수, 사용자 정의 데이터 등)
    """

    run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")
    # CURRENT_DATE가 datetime 객체면 그대로, 아니면 str을 파싱
    if isinstance(CURRENT_DATE, datetime):
        forced_date = CURRENT_DATE
    else:
        forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    start_date = (START_DATE + timedelta(weeks=((forced_date - START_DATE).days // 7) - 1)).strftime("%Y-%m-%d")

    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    next_date = (datetime.strptime(CURRENT_DATE, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d")

    return {
        "run_state": run_state,
        "forced_date": forced_date,
        "start_date": start_date,
        "end_date": end_date,
        "next_date": next_date
    }

def update_variable(context, RUN_STATE_FLAG):
    next_date = context["next_date"]
    
    Variable.set(RUN_STATE_FLAG, "done")
    Variable.set("current_date", next_date)
    logger.info(f"Variable 'current_date'가 {next_date}(으)로 업데이트 되었습니다.")