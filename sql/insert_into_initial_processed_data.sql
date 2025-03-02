/* 1. Raw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재 */

/* TIME_DIM: 실제 시간 관련 값은 속성 (Attribute)으로 저장, time_id를 Primary Key로 사용 */
INSERT INTO processed_data.time_dim (event_time, event_time_ymd, event_time_hms, event_time_month, event_time_day, event_time_hour, event_time_day_name)
SELECT DISTINCT event_time, event_time_ymd, event_time_hms, event_time_month, event_time_day, event_time_hour, event_time_day_name
FROM raw_data.raw_data;

/* CATEGORY_DIM: 제품의 카테고리 정보에 대한 상세 속성을 포함 */
INSERT INTO processed_data.category_dim (category_id, category_code, category_lv_1, category_lv_2, category_lv_3)
SELECT DISTINCT category_id, category_code, category_lv_1, category_lv_2, category_lv_3
FROM raw_data.raw_data;

/* PRODUCT_DIM 제품 관련 정보를 관리하며, product_id를 Primary Key로 사용 */
INSERT INTO processed_data.product_dim (product_id, brand, price)
SELECT DISTINCT product_id, brand, price
FROM raw_data.raw_data;

/* USER_DIM: 사용자 정보와 관련된 속성을 저장하며, user_id를 Primary Key로 사용*/
INSERT INTO processed_data.user_dim (user_id, user_session)
SELECT DISTINCT user_id, user_session
FROM raw_data.raw_data;

/* Fact Table: 핵심 이벤트 정보 (event_type, event_time, product_id, user_id 등)만을 포함 */
INSERT INTO processed_data.fact (time_id, category_id, product_id, user_id, user_session, event_type)
SELECT t.time_id,
       r.category_id,
       r.product_id,
       r.user_id,
       r.user_session,
       r.event_type
FROM raw_data.raw_data AS r
INNER JOIN processed_data.time_dim AS t
  ON r.event_time = t.event_time
  AND r.event_time_ymd = t.event_time_ymd
  AND r.event_time_hms = t.event_time_hms
  AND r.event_time_month = t.event_time_month
  AND r.event_time_day = t.event_time_day
  AND r.event_time_hour = t.event_time_hour
  AND r.event_time_day_name = t.event_time_day_name;
