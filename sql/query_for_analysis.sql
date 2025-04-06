-- 1-1. 요일별 및 시간대별 event_type 분포
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.EVENT_TYPE_TIME_DISTRIBUTION;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.EVENT_TYPE_TIME_DISTRIBUTION AS
SELECT 
    ft.event_time_month,
    ft.event_time_day_name,
    ft.event_time_hour,
    ft.event_type,
    COUNT(ft.user_session) AS user_session_count
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS ft
GROUP BY ft.event_time_month, ft.event_time_day_name, ft.event_time_hour, ft.event_type;


-- 1-2. CVR
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.CONVERSION_RATE;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.CONVERSION_RATE AS
SELECT 
    event_time_ymd, 
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS view_count,
    ROUND(
        (COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)) / NULLIF(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 0),
        4
    ) AS conversion_rate_percentage
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY event_time_ymd;


-- 1-3. User_id 별 Weekly Retention 분석
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.WEEKLY_RETENTION;
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.WEEKLY_RETENTION AS
SELECT *
FROM ECOMMERCE_DATA.STAGING_DATA.WEEKLY_RETENTION;


-- 1-4. User_id 별 Monthly Retention 분석
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.MONTHLY_RETENTION;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.MONTHLY_RETENTION AS
WITH monthly_cohorts AS (
    SELECT
        DATE_TRUNC('month', cohort_date) AS cohort_month,
        month_diff,
        SUM(retained_users) AS total_retained_users
    FROM ECOMMERCE_DATA.STAGING_DATA.MONTHLY_RETENTION
    GROUP BY DATE_TRUNC('month', cohort_date), month_diff
),
cohort_base AS (
    SELECT
        cohort_month,
        total_retained_users AS base_users
    FROM monthly_cohorts
    WHERE month_diff = 0
)
SELECT
    m.cohort_month,
    'Month ' || m.month_diff AS month_label,
    m.total_retained_users AS retained_users,
    ROUND(CAST(m.total_retained_users AS FLOAT) / c.base_users, 3) AS retention_rate_pct
FROM monthly_cohorts m
JOIN cohort_base c ON m.cohort_month = c.cohort_month
ORDER BY m.cohort_month, m.month_diff;


-- 1-5. Calendar Week 방식을 사용해서 wau, mau, stickness
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.ACTIVE_USERS;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.ACTIVE_USERS AS
SELECT *
FROM ECOMMERCE_DATA.STAGING_DATA.DAU
LEFT JOIN ECOMMERCE_DATA.STAGING_DATA.WAU
    USING (event_date)
LEFT JOIN ECOMMERCE_DATA.STAGING_DATA.MAU
    USING (event_date);


-- 1-6. stickiness & mau
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.STICKINESS;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.STICKINESS AS
SELECT *
FROM ECOMMERCE_DATA.STAGING_DATA.MAU
JOIN ECOMMERCE_DATA.STAGING_DATA.STICKINESS
    USING (event_date);

SELECT *
FROM ECOMMERCE_DATA.STAGING_DATA.STICKINESS;


-- 1-7. 퍼널 분석
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.FUNNEL;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.FUNNEL AS
SELECT DATE_TRUNC('month', event_time) as month, event_type, COUNT(event_type) AS cnt
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY DATE_TRUNC('month', event_time), event_type;


-- 1-8. 총 방문 수
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.COUNT_VISITORS;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.COUNT_VISITORS AS
SELECT event_time_ymd, COUNT(DISTINCT user_session) as user_session_count
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY event_time_ymd;

-- 1-9. RFM Segment 분석
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.SEGMENTED_RFM AS 
SELECT
    USER_ID,
    recency,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total,
    rfm_value_segment,
    customer_segment,
    -- 조합된 세그먼트 (예: High-Value - Champions)
    CONCAT(rfm_value_segment, ' - ', customer_segment) AS combined_segment
FROM ECOMMERCE_DATA.staging_data.Segmented_RFM;


-- 2-1. 결제된 Order 및 월별 결제 금액 합계
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.PURCHASED_ORDER;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.PURCHASED_ORDER AS
SELECT 
  event_time_ymd,
  SUM(price) AS daily_price,
  SUM(SUM(price)) OVER () AS total_price,
  COUNT(DISTINCT user_session) AS session_purchase_count
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
WHERE event_type = 'purchase'
GROUP BY event_time_ymd
ORDER BY event_time_ymd;



-- 2-2. 요일별 및 시간대별 결제 금액 분포
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.PRICE_DISTRIBUTION;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.PRICE_DISTRIBUTION AS
SELECT 
    ft.event_time_month,
    ft.event_time_day_name,
    ft.event_time_hour,
    ft.event_type,
    SUM(ft.price) AS hourly_price
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS ft
WHERE event_type = 'purchase'
GROUP BY ft.event_time_month, ft.event_time_day_name, ft.event_time_hour, ft.event_type;


-- 2-3. 월별 event_type 분포 및 결제 금액
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.MONTHLY_PRICE_EVENT_TYPE;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.MONTHLY_PRICE_EVENT_TYPE AS
WITH monthly_price AS (
    SELECT DATE_TRUNC('month', event_time) AS month,
        SUM(price) as sum_price
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS ft
    WHERE event_type = 'purchase'
    GROUP BY DATE_TRUNC('month', event_time)
),
monthly_event_type AS (
    SELECT DATE_TRUNC('month', event_time) AS month,
        event_type,
        COUNT(event_type) AS event_type_count
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS ft
    GROUP BY DATE_TRUNC('month', event_time), event_type
)
SELECT met.month, met.event_type, met.event_type_count, mp.sum_price
FROM monthly_event_type AS met
LEFT JOIN monthly_price AS mp
    ON met.month = mp.month;




-- 2-4. 마지막으로 저장한 데이터 일자
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.LATEST_DATE AS
SELECT MAX(event_time) AS latest_event_time
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME;


-- 3-1. Category 중분류 별 판매 순위 Top 10
DROP TABLE IF EXISTS ECOMMERCE_DATA.ANALYSIS.CATEGORY_PRICE_LV2;

CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.CATEGORY_PRICE_LV2 AS
WITH aggregated_data AS (
    SELECT DATE_TRUNC('month', event_time) AS month, category_lv_2, brand, SUM(price) AS sum_price
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_type = 'purchase' 
        AND category_lv_2 != 'unknown'
    GROUP BY DATE_TRUNC('month', event_time), category_lv_2, brand
)
SELECT month, category_lv_2, brand, sum_price
FROM (
    SELECT month, category_lv_2, brand, sum_price,
           ROW_NUMBER() OVER (PARTITION BY month ORDER BY sum_price DESC) AS rn
    FROM aggregated_data
) sub
WHERE rn <= 10
ORDER BY month, rn;

SELECT month, sum(sum_price)
FROM ECOMMERCE_DATA.ANALYSIS.CATEGORY_PRICE_LV2
GROUP BY month;






