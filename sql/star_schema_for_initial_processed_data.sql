/* 
1. Star Schema에 기반하여, Processed Data 스키마에 여러 테이블 생성

- Fact 테이블:
핵심 이벤트 정보 (event_type, event_time, product_id, user_id 등)만을 포함하여, 분석 시 집계 및 계산 작업의 효율성 증대 목적

- Dimension 테이블:
제품, 시간, 사용자, 카테고리 관련 세부 정보를 포함하여, 다차원 분석(Multi-dimensional Analysis)에 적합한 구조로 전환 목적.
*/

/* TIME_DIM: 실제 시간 관련 값은 속성 (Attribute)으로 저장, time_id를 Primary Key로 사용 */
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM (
    time_id INTEGER AUTOINCREMENT,
    event_time TIMESTAMP_NTZ,
    event_time_ymd DATE,
    event_time_hms TIME,
    event_time_month TEXT,
    event_time_day TEXT,
    event_time_hour TEXT,
    event_time_day_name TEXT
);

/* CATEGORY_DIM: 제품의 카테고리 정보에 대한 상세 속성을 포함 */
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM (
    category_id TEXT,
    category_code TEXT,
    category_lv_1 TEXT,
    category_lv_2 TEXT,
    category_lv_3 TEXT
);

/* PRODUCT_DIM 제품 관련 정보를 관리하며, product_id를 Primary Key로 사용 */
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM (
    product_id TEXT,
    brand TEXT,
    price FLOAT
);

/* USER_DIM: 사용자 정보와 관련된 속성을 저장하며, user_id를 Primary Key로 사용*/
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.USER_DIM (
    user_id TEXT,
    user_session TEXT
);

/* Fact Table: 핵심 이벤트 정보 (event_type, event_time, product_id, user_id 등)만을 포함 */
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.FACT (
    event_id INTEGER AUTOINCREMENT,
    time_id INTEGER,
    category_id TEXT,
    product_id TEXT,
    user_id TEXT,
    user_session TEXT,
    event_type TEXT
);
