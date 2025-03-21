import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging


# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQL 파일 경로 설정
SQL_DIR = "/opt/airflow/sql"

# Snowflake SQL 파일 실행 함수
def run_sql_task(sql_filename, task_type):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    """
    
    sql_path = os.path.join(SQL_DIR, sql_filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} 파일을 찾을 수 없습니다.")
    
    with open(sql_path, "r") as sql_file:
        sql_queries = sql_file.read()

    # Snowflake 연결 및 SQL 실행
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    try:
        # Snowflake 연결
        logger.info(f"SQL 쿼리 파일 실행 중 ({task_type}): {sql_filename}")
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()

        # SQL 문을 세미콜론으로 분리하여 개별적으로 실행
        statements = [query.strip() for query in sql_queries.split(';') if query.strip()]
        for idx, statement in enumerate(statements, start=1):
            logger.info(f"실행 중인 SQL 쿼리 {idx}/{len(statements)}: {statement[:200]}...")
            cursor.execute(statement)
            logger.info(f"Statement {idx} affected {cursor.rowcount} rows.")

        logger.info(f"모든 SQL 문을 성공적으로 실행했습니다. ({task_type}): {sql_filename}")

    except Exception as e:
        # 실행 중 에러 발생 시 로그 기록 및 재전파
        logger.error(f"SQL 파일 실행 중 오류 발생 ({task_type}): {sql_filename}: {str(e)}")
        raise e

    finally:
        # 커넥션 종료
        cursor.close()
        snowflake_conn.close()

def initial_run_sql(initial_sql_filename, schema="RAW_DATA"):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    run_state에 따라 초기 작업 -> schedule 작업 순으로 실행하거나, schedule 작업만 실행합니다.
    """

    if not initial_sql_filename:
        raise ValueError("schedule 작업을 위해 initial_sql_filename이 필요합니다.")

    logger.info(f"작업 중인 Snowflake Schema: {schema}")

    # 초기 작업 (공통 처리)
        
    reset_mode = "initial reset"
    logger.info(f"Snowflake {schema} 스키마의 Initial 작업을 수행합니다.    ({reset_mode})")
    run_sql_task(initial_sql_filename, reset_mode)