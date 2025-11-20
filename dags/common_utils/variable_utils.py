from datetime import datetime
from airflow.models import Variable

# 변수 전역 설정
START_DATE     = datetime(2019, 11, 26)
RUN_STATE_FLAG = "dag_run_state"

# 매 실행마다 바뀌는 기준일자(Variable)
# CURRENT_DATE   = Variable.get("current_date", default_var="2019-12-03")