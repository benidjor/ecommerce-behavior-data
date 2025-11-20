# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"

# 최종 컬럼 순서
NEW_COLUMN_ORDER = [
    "user_id", "user_session", "category_id", "event_time", "event_time_ymd", "event_time_hms", "event_time_month", "event_time_day", "event_time_hour", "event_time_day_name", "event_type", "product_id", "category_code", "category_lv_1", "category_lv_2", "category_lv_3",  "brand", "price"
]

INITIAL_CONTEXT = {
    "start_date": "2019-10-01",
    "end_date": "2019-11-25"
}