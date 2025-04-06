# plugins/tasks/snowflake_tasks.py
import os
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from custom_modules.config.snowflake_config import SQL_DIR
from custom_modules.utils.snowflake_helpers import render_sql_template
from airflow.decorators import task

logger = logging.getLogger(__name__)

def run_sql_task(sql_filename, task_type, context=None):
    """
    SQL 파일을 읽어 Jinja2 템플릿으로 렌더링 후, SnowflakeHook를 통해 실행합니다.
    
    :param sql_filename: 실행할 SQL 파일 이름
    :param task_type: 작업 유형(예: initial reset)
    :param context: 템플릿에 전달할 변수 딕셔너리 (예: {'start_date': '2023-06-11', 'end_date': '2023-06-18'})
    """

    sql_path = os.path.join(SQL_DIR, sql_filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} 파일을 찾을 수 없습니다.")
    
    # Jinja2 템플릿 로드 및 렌더링
    rendered_sql = render_sql_template(sql_filename, context)

    logger.info(f"데이터 적재 시작 날짜 {context['start_date']}")

    # Snowflake 연결 및 SQL 실행
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    try:
        logger.info(f"SQL 실행 시작 ({task_type}): {sql_filename}")
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()

        # 렌더링된 SQL 문을 세미콜론(;)으로 분리하여 개별적으로 실행합니다.
        statements = [stmt.strip() for stmt in rendered_sql.split(';') if stmt.strip()]

        for idx, statement in enumerate(statements, start=1):
            logger.info(f"SQL 문 {idx}/{len(statements)} 실행: {statement[:200]}...")
            cursor.execute(statement)
            logger.info(f"Statement {idx} -> {cursor.rowcount} 행 영향")
        logger.info(f"모든 SQL 문 실행 완료 ({task_type}): {sql_filename}")

    except Exception as e:
        logger.error(f"SQL 실행 중 오류 발생 ({task_type}): {sql_filename}: {e}")
        raise e
    
    finally:
        cursor.close()
        snowflake_conn.close()

@task
def execute_sql_task(sql_filename: str, schema: str = "RAW_DATA", task_type: str = "initial", context: dict = None) -> str:
    """
    SQL 파일을 읽어 Jinja2 템플릿으로 렌더링 후, SnowflakeHook를 통해 실행합니다.
    run_state에 따라 초기 작업 -> schedule 작업 순으로 실행하거나, schedule 작업만 실행합니다.
    """
    
    if not sql_filename:
        raise ValueError("sql_filename이 필요합니다.")
    
    logger.info(f"작업 중인 Snowflake Schema: {schema}")

    logger.info(f"Snowflake {schema} 초기 작업 수행 ({task_type})")
    run_sql_task(sql_filename, task_type, context)
    return f"SQL task {sql_filename} completed"
