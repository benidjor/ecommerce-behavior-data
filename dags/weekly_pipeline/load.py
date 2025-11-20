import logging

from airflow.models import Variable
from common_utils.variable_utils import START_DATE, RUN_STATE_FLAG
from common_utils.date_utils import get_task_context
from common_utils.snowflake_utils import initial_or_weekly_run_sql

logger = logging.getLogger(__name__)



def copy_data_from_s3(**kwargs):
    """
    Airflow Variable current_date를 매 실행 시점에 읽어
    context를 생성한 뒤 SQL을 렌더링/실행합니다.
    """
     # 공통 variable 모듈에서 가져온 변수 사용

    # 1) 실행 시점 최신 Variable 읽기
    current_date = Variable.get("current_date", default_var="2019-12-03")
    context = get_task_context(RUN_STATE_FLAG, current_date, START_DATE)
    initial_or_weekly_run_sql(
        sql_filename="weekly_copy_into_snowflake_from_s3.sql",
        schema="RAW_DATA",
        context=context
    )

def insert_data_to_star_schema_tables(context):
    """
    SnowflakeRaw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재
    """
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_or_weekly_run_sql(
        sql_filename="weekly_insert_into_initial_processed_data.sql",
        schema=schema,
        context=context
    )

def query_for_staging_or_analysis(schema, context):
    """
    PROCESSED_DATA Schema의 테이블을 활용하여, STAGING_DATA or ANALYSIS Schema에 테이블을 생성합니다.
    """
    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    
    if schema == "STAGING_DATA":
        initial_or_weekly_run_sql(
            sql_filename="query_for_staging_data.sql",
            schema=schema,
            context=context
        )
    else:
        initial_or_weekly_run_sql(
            sql_filename="query_for_analysis.sql",
            schema=schema,
            context=context
        )
