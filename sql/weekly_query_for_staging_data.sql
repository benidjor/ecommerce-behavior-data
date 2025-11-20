-- ============================================================
-- 1) FACT → STAGING: FACT_CATEGORY_PRODUCT_TIME 증분 MERGE
--    • PROCESSED_DATA.FACT 테이블에서 날짜 범위만 직접 조회
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS tgt
USING (
  SELECT
    f.time_id,
    f.event_id,
    f.category_id,
    f.user_id,
    f.user_session,
    f.event_type,
    f.product_id,
    c.category_code,
    c.category_lv_1,
    c.category_lv_2,
    c.category_lv_3,
    p.brand,
    p.price,
    t.event_time,
    t.event_time_ymd,
    t.event_time_hms,
    t.event_time_month,
    t.event_time_day,
    t.event_time_hour,
    t.event_time_day_name
  FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT AS f
  LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM AS c
    ON f.category_id = c.category_id
  LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM AS p
    ON f.product_id = p.product_id
  LEFT JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
    ON f.time_id = t.time_id
  WHERE t.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
) AS src
  ON tgt.event_id = src.event_id
WHEN NOT MATCHED THEN
  INSERT (
    time_id, event_id, category_id, user_id, user_session,
    event_type, product_id,
    category_code, category_lv_1, category_lv_2, category_lv_3,
    brand, price,
    event_time, event_time_ymd, event_time_hms,
    event_time_month, event_time_day, event_time_hour, event_time_day_name
  )
  VALUES (
    src.time_id, src.event_id, src.category_id, src.user_id, src.user_session,
    src.event_type, src.product_id,
    src.category_code, src.category_lv_1, src.category_lv_2, src.category_lv_3,
    src.brand, src.price,
    src.event_time, src.event_time_ymd, src.event_time_hms,
    src.event_time_month, src.event_time_day, src.event_time_hour, src.event_time_day_name
  );

-- ============================================================
-- 2) RETENTION: 일간 Retention 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.RETENTION AS tgt
USING (
  WITH first_event AS (
    SELECT 
      user_id,
      MIN(TO_DATE(event_time_ymd)) AS first_date
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY user_id
  ),
  user_events AS (
    SELECT 
      fe.user_id,
      fe.first_date,
      TO_DATE(t.event_time_ymd)       AS event_date,
      DATEDIFF(day, fe.first_date, TO_DATE(t.event_time_ymd)) AS day_diff
    FROM first_event fe
    JOIN ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS f
      ON fe.user_id = f.user_id
    JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
      ON f.time_id = t.time_id
    WHERE f.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  )
  SELECT
    ue.first_date   AS cohort_date,
    ue.day_diff     AS day_diff,
    COUNT(DISTINCT ue.user_id) AS retained_users
  FROM user_events ue
  GROUP BY ue.first_date, ue.day_diff
) AS src
  ON tgt.cohort_date = src.cohort_date
 AND tgt.day_diff     = src.day_diff
WHEN MATCHED THEN
  UPDATE SET retained_users = src.retained_users
WHEN NOT MATCHED THEN
  INSERT (cohort_date, day_diff, retained_users)
  VALUES (src.cohort_date, src.day_diff, src.retained_users);

-- ============================================================
-- 3) WEEKLY_RETENTION: 주간 Retention 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.WEEKLY_RETENTION AS tgt
USING (
  WITH first_event AS (
    SELECT 
      user_id,
      MIN(event_time) AS first_event_date
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY user_id
  ),
  retention_by_week AS (
    SELECT
      TO_CHAR(f.first_event_date, 'YYYY-MM')            AS cohort_month,
      DATEDIFF('week', f.first_event_date, s.event_time) AS week_number,
      COUNT(DISTINCT s.user_id)                          AS active_users
    FROM first_event f
    JOIN ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS s
      ON f.user_id = s.user_id
    WHERE s.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY 1,2
  ),
  cohort AS (
    SELECT
      TO_CHAR(first_event_date, 'YYYY-MM') AS cohort_month,
      COUNT(DISTINCT user_id)             AS cohort_size
    FROM first_event
    GROUP BY TO_CHAR(first_event_date, 'YYYY-MM')
  )
  SELECT
    w.cohort_month,
    w.week_number,
    w.active_users,
    c.cohort_size,
    ROUND(w.active_users * 100.0 / c.cohort_size, 2) AS retention_rate_percentage
  FROM retention_by_week w
  JOIN cohort c
    ON w.cohort_month = c.cohort_month
) AS src
  ON tgt.cohort_month = src.cohort_month
 AND tgt.week_number  = src.week_number
WHEN MATCHED THEN
  UPDATE SET
    active_users              = src.active_users,
    cohort_size               = src.cohort_size,
    retention_rate_percentage = src.retention_rate_percentage
WHEN NOT MATCHED THEN
  INSERT (
    cohort_month, week_number, active_users, cohort_size, retention_rate_percentage
  )
  VALUES (
    src.cohort_month, src.week_number, src.active_users, src.cohort_size, src.retention_rate_percentage
  );

-- ============================================================
-- 4) MONTHLY_RETENTION: 월간 Retention 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.MONTHLY_RETENTION AS tgt
USING (
  WITH first_event AS (
    SELECT 
      user_id,
      MIN(TO_DATE(event_time_ymd)) AS first_date
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY user_id
  ),
  retention_by_month AS (
    SELECT
      fe.first_date   AS cohort_date,
      DATEDIFF('month', fe.first_date, TO_DATE(t.event_time_ymd)) AS month_diff,
      COUNT(DISTINCT f.user_id)          AS retained_users
    FROM first_event fe
    JOIN ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS f
      ON fe.user_id = f.user_id
    JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
      ON f.time_id = t.time_id
    WHERE f.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY fe.first_date, month_diff
  )
  SELECT * FROM retention_by_month
) AS src
  ON tgt.cohort_date = src.cohort_date
 AND tgt.month_diff  = src.month_diff
WHEN MATCHED THEN
  UPDATE SET retained_users = src.retained_users
WHEN NOT MATCHED THEN
  INSERT (cohort_date, month_diff, retained_users)
  VALUES (src.cohort_date, src.month_diff, src.retained_users);

-- ============================================================
-- 5) DAU: 일간 Active User 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.DAU AS tgt
USING (
  SELECT
    CAST(event_time AS DATE) AS event_date,
    COUNT(DISTINCT user_id)  AS dau
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY CAST(event_time AS DATE)
) AS src
  ON tgt.event_date = src.event_date
WHEN MATCHED THEN
  UPDATE SET dau = src.dau
WHEN NOT MATCHED THEN
  INSERT (event_date, dau)
  VALUES (src.event_date, src.dau);

-- ============================================================
-- 6) WAU: 주간 Active User 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.WAU AS tgt
USING (
  SELECT
    DATE_TRUNC('week', event_time) AS event_date,
    COUNT(DISTINCT user_id)        AS wau
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY DATE_TRUNC('week', event_time)
) AS src
  ON tgt.event_date = src.event_date
WHEN MATCHED THEN
  UPDATE SET wau = src.wau
WHEN NOT MATCHED THEN
  INSERT (event_date, wau)
  VALUES (src.event_date, src.wau);

-- ============================================================
-- 7) MAU: 월간 Active User 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.MAU AS tgt
USING (
  SELECT
    DATE_TRUNC('month', event_time) AS event_date,
    COUNT(DISTINCT user_id)         AS mau
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY DATE_TRUNC('month', event_time)
) AS src
  ON tgt.event_date = src.event_date
WHEN MATCHED THEN
  UPDATE SET mau = src.mau
WHEN NOT MATCHED THEN
  INSERT (event_date, mau)
  VALUES (src.event_date, src.mau);

-- ============================================================
-- 8) STICKINESS: 증분 Upsert
-- ============================================================
MERGE INTO ECOMMERCE_DATA.STAGING_DATA.STICKINESS AS tgt
USING (
  WITH daily AS (
    SELECT
      CAST(event_time AS DATE) AS event_date,
      COUNT(DISTINCT user_id)  AS daily_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY CAST(event_time AS DATE)
  ),
  monthly AS (
    SELECT
      DATE_TRUNC('month', event_time) AS event_month,
      COUNT(DISTINCT user_id)         AS monthly_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('month', event_time)
  ),
  daily_avg AS (
    SELECT
      DATE_TRUNC('month', event_date) AS event_month,
      AVG(daily_active_users)        AS avg_daily_active_users
    FROM daily
    GROUP BY DATE_TRUNC('month', event_date)
  )
  SELECT
    CAST(m.event_month AS DATE)                                  AS event_date,
    (d.avg_daily_active_users::FLOAT / m.monthly_active_users)  AS stickiness
  FROM monthly m
  JOIN daily_avg d
    ON m.event_month = d.event_month
) AS src
  ON tgt.event_date = src.event_date
WHEN MATCHED THEN
  UPDATE SET stickiness = src.stickiness
WHEN NOT MATCHED THEN
  INSERT (event_date, stickiness)
  VALUES (src.event_date, src.stickiness);

-- ============================================================
-- 9) SEGMENTED_RFM: 증분 Upsert (CTE를 통한 RFM 계산)[^1]
-- ============================================================
-- MERGE INTO ECOMMERCE_DATA.STAGING_DATA.SEGMENTED_RFM AS tgt
-- USING (
--   WITH rfm_calc AS (
--     SELECT
--       f.user_id,
--       DATEDIFF('day', MAX(f.event_time), '{{ end_date }}') AS recency,
--       COUNT(*)                           AS frequency,
--       SUM(f.price)                      AS monetary
--     FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS f
--     WHERE f.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
--     GROUP BY f.user_id
--   ),
--   rfm_scores AS (
--     SELECT
--       user_id,
--       recency,
--       frequency,
--       monetary,
--       NTILE(5) OVER (ORDER BY recency ASC)        AS recency_score,
--       NTILE(5) OVER (ORDER BY frequency DESC)     AS frequency_score,
--       NTILE(5) OVER (ORDER BY monetary DESC)      AS monetary_score
--     FROM rfm_calc
--   ),
--   rfm_final AS (
--     SELECT
--       user_id,
--       recency,
--       frequency,
--       monetary,
--       recency_score,
--       frequency_score,
--       monetary_score,
--       recency_score + frequency_score + monetary_score AS rfm_total,
--       CASE
--         WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
--         WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal'
--         ELSE 'Others'
--       END AS customer_segment
--     FROM rfm_scores
--   )
--   SELECT * FROM rfm_final
-- ) AS src
--   ON tgt.USER_ID = src.USER_ID
-- WHEN MATCHED THEN
--   UPDATE SET
--     recency           = src.recency,
--     frequency         = src.frequency,
--     monetary          = src.monetary,
--     recency_score     = src.recency_score,
--     frequency_score   = src.frequency_score,
--     monetary_score    = src.monetary_score,
--     rfm_total         = src.rfm_total,
--     customer_segment  = src.customer_segment
-- WHEN NOT MATCHED THEN
--   INSERT (
--     USER_ID, recency, frequency, monetary,
--     recency_score, frequency_score, monetary_score,
--     rfm_total, customer_segment
--   )
--   VALUES (
--     src.USER_ID, src.recency, src.frequency, src.monetary,
--     src.recency_score, src.frequency_score, src.monetary_score,
--     src.rfm_total, src.customer_segment
--   )

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
