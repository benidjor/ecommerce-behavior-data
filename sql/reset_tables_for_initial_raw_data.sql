/* s3 initial 폴더 업데이트 후 기존 데이터를 drop하고 새로 업데이트 적용 */
DROP TABLE IF EXISTS ECOMMERCE_DATA.RAW_DATA.RAW_DATA;

-- Raw data 테이블
CREATE OR REPLACE TABLE ECOMMERCE_DATA.RAW_DATA.RAW_DATA (
    user_id TEXT,
    user_session TEXT,
    category_id TEXT,
    event_time TIMESTAMP_NTZ,
    event_time_ymd DATE,
    event_time_hms TIME,
    event_time_month TEXT,
    event_time_day TEXT,
    event_time_hour TEXT,
    event_time_day_name TEXT,
    event_type TEXT,
    product_id TEXT,
    category_code TEXT,
    category_lv_1 TEXT,
    category_lv_2 TEXT,
    category_lv_3 TEXT,
    brand TEXT,
    price FLOAT
);