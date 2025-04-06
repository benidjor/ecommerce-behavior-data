import logging
from datetime import datetime, timedelta
from airflow.models import Variable

logger = logging.getLogger(__name__)

RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable
CURRENT_DATE_DEFAULT = "2019-12-03"
START_DATE = datetime(2019, 11, 26)

# 변수를 동적으로 가져오는 함수
def get_airflow_variable(name, default=None):
    """
    주어진 이름의 Airflow 변수를 가져옵니다. 없으면 기본값을 반환합니다.
    :param name: Airflow 변수 이름
    :param default: 기본값 (변수가 없을 경우 반환)
    :return: 변수 값 또는 기본값
    """
    try:
        return Variable.get(name, default_var=default)
    except KeyError:
        return default


def get_task_context(start_date=START_DATE):
    """
    DAG 실행을 위한 컨텍스트를 반환합니다.
    - 현재 날짜는 Airflow Variable에서 가져오며, 기본값은 CURRENT_DATE_DEFAULT입니다.
    - START_DATE부터 강제 날짜(forced_date)까지의 주차를 계산하여 시작/종료 날짜, 다음 날짜를 산출합니다.
    """
    # config.py에서 가져온 값 사용
    run_state = get_airflow_variable(RUN_STATE_FLAG, default="pending")  # 기본값은 "pending"
    CURRENT_DATE = Variable.get("current_date", default_var="2023-06-11")

    # CURRENT_DATE는 문자열로 가정, 필요 시 변환
    if isinstance(CURRENT_DATE, str):
        forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    else:
        forced_date = CURRENT_DATE
    
    # start_date가 datetime인지 확인하고 문자열로 변환
    if isinstance(start_date, datetime):
        start_date_str = start_date.strftime("%Y-%m-%d")
    else:
        start_date_str = start_date

    # CURRENT_DATE = Variable.get("current_date", default_var=CURRENT_DATE_DEFAULT)
    # run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")
    forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    # start_date = (START_DATE + timedelta(weeks=((forced_date - START_DATE).days // 7) - 1)).strftime("%Y-%m-%d")
    # end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    # next_date = (datetime.strptime(CURRENT_DATE, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d")
    start_date = (
        datetime.strptime(start_date_str, "%Y-%m-%d") +
        timedelta(
            weeks=(
                (datetime.strptime(forced_date.strftime("%Y-%m-%d"), "%Y-%m-%d") -
                 datetime.strptime(start_date_str, "%Y-%m-%d")
                ).days // 7
            ) - 1
        )
    ).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    next_date = (forced_date + timedelta(weeks=1)).strftime("%Y-%m-%d")
    return {
        "run_state": run_state,
        "forced_date": forced_date,
        "start_date": start_date,
        "end_date": end_date,
        "next_date": next_date
    }

def reset_variable(**kwargs):
    """
    DAG 최초 실행 시 또는 강제 초기화 시 Variable 및 S3 데이터 초기화를 수행합니다.
    """
    context = get_task_context()
    run_state = context["run_state"]
    dag_run = kwargs.get("dag_run")
    if dag_run:
        logger.info(f"DAG run_type: {dag_run.run_type}, RUN_STATE_FLAG: {run_state}")
    
    if run_state in ["pending", "force_reset"]:
        logger.info("Variable 및 S3 데이터 초기화를 수행합니다.")
        Variable.set("current_date", CURRENT_DATE_DEFAULT)
        logger.info("Variable 초기화가 완료되었습니다.")
    else:
        logger.info("초기화 작업을 건너뜁니다. DAG가 이전에 이미 실행되었습니다.")

def update_variable():
    """
    DAG 작업 완료 후 다음 주의 날짜로 Variable을 업데이트합니다.
    """
    context = get_task_context()
    next_date = context["next_date"]
    Variable.set(RUN_STATE_FLAG, "done")
    Variable.set("current_date", next_date)
    logger.info(f"Variable 'current_date'가 {next_date}(으)로 업데이트 되었습니다.")
