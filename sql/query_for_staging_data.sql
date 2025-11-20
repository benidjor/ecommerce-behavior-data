-- FACT 테이블과 CATEGORY_DIM, PRODUCT_DIM, TIME_DIM 테이블 JOIN
-- DROP TABLE IF EXISTS ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME

CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS
SELECT f.time_id, f.event_id, f.category_id, f.user_id, f.user_session, f.event_type, f.product_id, 
    c.CATEGORY_CODE, c.category_lv_1, c.category_lv_2, c.category_lv_3, 
    p.brand, p.price,
    t.event_time, t.event_time_ymd, t.event_time_hms, t.event_time_month, t.event_time_day, t.event_time_hour, t.event_time_day_name
FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM AS c
    USING (category_id)
LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM AS p
    USING (product_id)
LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
    USING (time_id);

-- RETENTION 테이블 생성
-- DROP TABLE IF EXISTS ECOMMERCE_DATA.STAGING_DATA.RETENTION

CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.RETENTION AS
WITH first_event AS (
    SELECT 
        f.user_id,
        MIN(TO_DATE(t.event_time_ymd)) AS first_date
    FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
    LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
        USING (time_id)
    GROUP BY f.user_id
),
user_events AS (
    SELECT 
        f.user_id,
        fe.first_date,
        t.event_time_ymd AS event_date,
        DATEDIFF(day, fe.first_date, TO_DATE(t.event_time_ymd)) AS day_diff
    FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
    LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
        USING (time_id)
    JOIN first_event AS fe
      ON f.user_id = fe.user_id
)
SELECT 
    first_date AS cohort_date,
    day_diff,
    COUNT(DISTINCT user_id) AS retained_users
FROM user_events
GROUP BY 1, 2
ORDER BY 1, 2;


-- Weekly Retention 테이블
-- DROP TABLE IF EXISTS ECOMMERCE_DATA.STAGING_DATA.WEEKLY_RETENTION

CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.WEEKLY_RETENTION AS
WITH first_event AS (
    SELECT 
        user_id, 
        MIN(event_time) AS first_event_date
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    GROUP BY user_id
),
retention AS (
    SELECT
        f.user_id,
        f.first_event_date,
        t.event_time,
        -- 첫 이벤트 날짜를 기준으로 주차 계산 (0부터 시작)
        DATEDIFF('week', f.first_event_date, t.event_time) AS week_number,
        -- 첫 이벤트 월을 코호트로 정의
        TO_CHAR(f.first_event_date, 'YYYY-MM') AS cohort_month
    FROM first_event f
    JOIN ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME t
        ON f.user_id = t.user_id
),
cohort AS (
    SELECT 
        TO_CHAR(first_event_date, 'YYYY-MM') AS cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM first_event
    GROUP BY TO_CHAR(first_event_date, 'YYYY-MM')
),
retention_by_week AS (
    SELECT 
        cohort_month,
        week_number,
        COUNT(DISTINCT user_id) AS active_users
    FROM retention
    WHERE week_number BETWEEN 0 AND 4
    GROUP BY cohort_month, week_number
)
SELECT
    r.cohort_month,
    r.week_number,
    r.active_users,
    c.cohort_size,
    ROUND((r.active_users * 100.0 / c.cohort_size), 2) AS retention_rate_percentage
FROM retention_by_week r
JOIN cohort c ON r.cohort_month = c.cohort_month
ORDER BY r.cohort_month, r.week_number;


-- Monthly Retention 테이블
-- DROP TABLE IF EXISTS ECOMMERCE_DATA.STAGING_DATA.MONTHLY_RETENTION

CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.MONTHLY_RETENTION AS
WITH first_event AS (
    SELECT 
        f.user_id,
        MIN(CAST(t.event_time_ymd AS DATE)) AS first_date
    FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
    LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
        USING (time_id)
    GROUP BY f.user_id
),
user_events AS (
    SELECT 
        f.user_id,
        fe.first_date,
        CAST(t.event_time_ymd AS DATE) AS event_date,
        DATEDIFF('month', fe.first_date, CAST(t.event_time_ymd AS DATE)) AS month_diff
    FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
    LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
        USING (time_id)
    JOIN first_event AS fe
      ON f.user_id = fe.user_id
)
SELECT 
    first_date AS cohort_date,
    month_diff,
    COUNT(DISTINCT user_id) AS retained_users
FROM user_events
GROUP BY 1, 2
ORDER BY 1, 2;


-- DAU (Daily Active User)
CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.DAU AS
SELECT 
    CAST(event_time AS DATE) AS event_date,
    COUNT(DISTINCT user_id) AS dau
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY CAST(event_time AS DATE)
ORDER BY event_date;


-- WAU (Weekly Active User)
CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.WAU AS
SELECT 
    CAST(
        DATEADD(
          day, 
          FLOOR(DATEDIFF(day, '2019-10-01', CAST(event_time AS DATE)) / 7) * 7, 
          '2019-10-01'
        ) AS DATE
    ) AS event_date,
    COUNT(DISTINCT user_id) AS wau
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY 
    DATEADD(
      day, 
      FLOOR(DATEDIFF(day, '2019-10-01', CAST(event_time AS DATE)) / 7) * 7, 
      '2019-10-01'
    )
ORDER BY event_date;


-- MAU (Monthly Active User)
CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.MAU AS
SELECT 
    CAST(DATE_TRUNC('month', event_time) AS DATE) AS event_date,
    COUNT(DISTINCT user_id) AS mau
FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
GROUP BY DATE_TRUNC('month', event_time)
ORDER BY event_date;


-- Stickness (DAU / MAU)
CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.STICKINESS AS
WITH daily AS (
    SELECT 
        CAST(event_time AS DATE) AS event_date,
        COUNT(DISTINCT user_id) AS daily_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    GROUP BY 1
),
monthly AS (
    SELECT 
        DATE_TRUNC('month', event_time) AS event_month,
        COUNT(DISTINCT user_id) AS monthly_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    GROUP BY 1
),
daily_avg AS (
    SELECT 
        DATE_TRUNC('month', event_date) AS event_month,
        AVG(daily_active_users) AS avg_daily_active_users
    FROM daily
    GROUP BY 1
)
SELECT 
    CAST(m.event_month AS DATE) as event_date,
    (d.avg_daily_active_users / m.monthly_active_users) AS stickiness
FROM monthly m
JOIN daily_avg d ON m.event_month = d.event_month
ORDER BY m.event_month;


-- RFM 분석용 테이블
CREATE OR REPLACE TABLE ECOMMERCE_DATA.staging_data.Segmented_RFM AS 
WITH RFM_Data AS (
    SELECT
        USER_ID,
        MAX(EVENT_TIME) AS last_purchase,
        COUNT(*) AS frequency,
        SUM(PRICE) AS monetary
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE EVENT_TYPE = 'purchase'
    GROUP BY USER_ID
),
Reference AS (
    SELECT MAX(EVENT_TIME) AS max_event_time
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
),
RFM_Calc AS (
    SELECT
        USER_ID,
        DATEDIFF(day, last_purchase, ref.max_event_time) AS recency,
        frequency,
        monetary
    FROM RFM_Data r
    CROSS JOIN Reference ref
),
Scored_RFM AS (
    SELECT
        USER_ID,
        recency,
        frequency,
        monetary,
        -- Recency: 낮을수록 좋으므로 ASC 정렬 후 역순 점수 (최고 5점)
        (6 - NTILE(5) OVER (ORDER BY recency ASC)) AS recency_score,
        -- Frequency: 높을수록 좋으므로 DESC 정렬 후 역순 점수 (최고 5점)
        (6 - NTILE(5) OVER (ORDER BY frequency DESC)) AS frequency_score,
        -- Monetary: 높을수록 좋으므로 DESC 정렬 후 역순 점수 (최고 5점)
        (6 - NTILE(5) OVER (ORDER BY monetary DESC)) AS monetary_score
    FROM RFM_Calc
)
SELECT
    USER_ID,
    recency,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) AS rfm_total,
    -- RFM Value Segment (3등급)
    CASE 
        WHEN (recency_score + frequency_score + monetary_score) >= 12 THEN 'High-Value'
        WHEN (recency_score + frequency_score + monetary_score) >= 8 THEN 'Mid-Value'
        ELSE 'Low-Value'
    END AS rfm_value_segment,
    -- Customer Segment (5세그먼트)
    CASE 
        WHEN recency_score = 1 THEN 'Inactive Customer'
        WHEN recency_score = 2 THEN 'At-Risk Customer'
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'VIP Customer'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Potential Loyalist'
        ELSE 'Core Customer'
    END AS customer_segment
FROM Scored_RFM;