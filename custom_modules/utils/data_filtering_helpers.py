import pandas as pd
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

def set_filtering_date(weekly_start_date="2019-10-01", weekly_end_date="2020-01-04", freq="7D"):
    """
    주 단위 날짜 범위를 생성하는 함수
    """
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
    특정 날짜 범위로 데이터프레임을 필터링하는 함수
    """
    try:
        if df.rdd.isEmpty():
            logger.warning("데이터프레임이 비어 있습니다.")
            return None
        ymd_columns = [col for col in df.columns if col.endswith("ymd")]
        if not ymd_columns:
            logger.error("필터링 기준 컬럼을 찾을 수 없습니다.")
            return None
        for col in ymd_columns:
            df = df.withColumn(col, F.to_date(df[col]))
        logger.info(f"필터링 기준 컬럼: {ymd_columns[0]}")
        filtered_df = df.filter(df[ymd_columns[0]].between(start_date, end_date))
        return filtered_df
    except Exception as e:
        logger.error(f"필터링 중 오류 발생: {e}")
        return None
