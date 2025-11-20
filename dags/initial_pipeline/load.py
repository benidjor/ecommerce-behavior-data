import logging
from common_utils.snowflake_utils import initial_or_weekly_run_sql

logger = logging.getLogger(__name__)

def reset_tables():
    """
    Snowflake ECOMMERCE_RAW_DATA Schema에 통합 데이터셋 적재할 테이블 생성
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_or_weekly_run_sql(
        sql_filename="reset_tables_for_initial_raw_data.sql",
        schema=schema,
    )

def copy_data_from_s3():
    """
    Snowflake ECOMMERCE_RAW_DATA Schema의 테이블에 통합 데이터셋 적재
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_or_weekly_run_sql(
        sql_filename="copy_into_snowflake_from_s3.sql",
        schema=schema,
    )

def star_schema_tables():
    """
    Star Schema에 기반하여, Snowflake ECOMMERCE_PROCESSED_DATA Schema에 테이블 생성
    """
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_or_weekly_run_sql(
        sql_filename="star_schema_for_initial_processed_data.sql",
        schema=schema,
    )

def insert_data_to_star_schema_tables(context):
    """
    SnowflakeRaw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재
    """
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_or_weekly_run_sql(
        sql_filename="insert_into_initial_processed_data.sql",
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
