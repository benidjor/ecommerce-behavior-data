import os
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from jinja2 import Environment, FileSystemLoader, Template
from datetime import datetime

from config.snowflake_config import SQL_DIR

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQL 파일 경로 설정
# SQL_DIR = "/opt/airflow/sql"

# Jinja2 환경 설정: SQL 파일이 있는 디렉토리를 템플릿 로더로 지정합니다.
env = Environment(loader=FileSystemLoader(SQL_DIR))


# Snowflake SQL 파일 실행 함수
def run_sql_task(sql_filename, sql_params=None):
    """
    SQL 파일을 읽어 Jinja2 템플릿으로 렌더링 후, SnowflakeHook를 통해 실행합니다.
    
    :param sql_filename: 실행할 SQL 파일 이름
    :param sql_params: 템플릿에 전달할 변수 딕셔너리 (예: {'start_date': '2023-06-11', 'end_date': '2023-06-18'})
    """
    
    sql_path = os.path.join(SQL_DIR, sql_filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} 파일을 찾을 수 없습니다.")
    
    # Jinja2 템플릿 로드 및 렌더링
    template = env.get_template(sql_filename)
    rendered_sql = template.render(sql_params or {})
    
    # Snowflake 연결 및 SQL 실행
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    
    try:
        logger.info(f"SQL 쿼리 파일 실행 중 {sql_filename}")
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()
        
        # 렌더링된 SQL 문을 세미콜론(;)으로 분리하여 개별적으로 실행합니다.
        logger.info(f"Rendered SQL:\n{rendered_sql}")
        statements = [query.strip() for query in rendered_sql.split(';') if query.strip()]
        for idx, statement in enumerate(statements, start=1):
            logger.info(f"실행 중인 SQL 쿼리 {idx}/{len(statements)}: {statement[:200]}...")
            cursor.execute(statement)
            logger.info(f"Statement {idx} affected {cursor.rowcount} rows.")
        
        logger.info(f"모든 SQL 문을 성공적으로 실행했습니다. {sql_filename}")
    
    except Exception as e:
        logger.error(f"SQL 파일 실행 중 오류 발생 {sql_filename}: {str(e)}")
        raise e
    
    finally:
        cursor.close()
        snowflake_conn.close()

def initial_or_weekly_run_sql(sql_filename, schema="RAW_DATA", context=None):
    """
    SQL 파일을 읽어 Jinja2 템플릿으로 렌더링 후, SnowflakeHook를 통해 실행합니다.
    run_state에 따라 초기 작업 -> schedule 작업 순으로 실행하거나, schedule 작업만 실행합니다.
    """
    if context:
        # 동적으로 사용할 날짜 변수
        sql_params = {
            "start_date": context["start_date"],
            "end_date": context["end_date"],
            "s3_prefix_filter" : datetime.strptime(context["start_date"], "%Y-%m-%d").strftime("%y%m%d")
        }
        logger.info(f"작업 기준 날짜: {sql_params}")
    else:
        sql_params = None    

    if not sql_filename:
        raise ValueError("Snowflake Data Warehouse ELT 작업을 위해 sql_filename이 필요합니다.")

    logger.info(f"작업 중인 Snowflake Schema: {schema}")
    
    # reset_mode = "initial reset"
    # logger.info(f"Snowflake {schema} 스키마의 Initial 작업을 수행합니다. ({reset_mode})")
    run_sql_task(sql_filename, sql_params=sql_params)
