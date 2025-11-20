-- ============================================================
-- weekly_query_for_analysis.sql
-- • STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME 테이블 직접 조회
-- • '{{ start_date }}' ~ '{{ end_date }}' 날짜 범위만 Incremental Merge
-- ============================================================

-- ============================================================
-- 1-1) EVENT_TYPE_TIME_DISTRIBUTION 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.EVENT_TYPE_TIME_DISTRIBUTION AS tgt
USING (
  SELECT
    event_time_month,
    event_time_day_name,
    event_time_hour,
    event_type,
    COUNT(user_session)         AS user_session_count
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY
    event_time_month,
    event_time_day_name,
    event_time_hour,
    event_type
) AS src
  ON   tgt.event_time_month    = src.event_time_month
    AND tgt.event_time_day_name = src.event_time_day_name
    AND tgt.event_time_hour     = src.event_time_hour
    AND tgt.event_type          = src.event_type
WHEN MATCHED THEN
  UPDATE SET user_session_count = src.user_session_count
WHEN NOT MATCHED THEN
  INSERT (
    event_time_month,
    event_time_day_name,
    event_time_hour,
    event_type,
    user_session_count
  )
  VALUES (
    src.event_time_month,
    src.event_time_day_name,
    src.event_time_hour,
    src.event_type,
    src.user_session_count
  );

-- ============================================================
-- 1-2) CONVERSION_RATE 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.CONVERSION_RATE AS tgt
USING (
  SELECT
    event_time_ymd,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS view_count,
    ROUND(
      COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)
      / NULLIF(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 0),
      4
    ) AS conversion_rate_percentage
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY event_time_ymd
) AS src
  ON tgt.event_time_ymd = src.event_time_ymd
WHEN MATCHED THEN
  UPDATE SET
    purchase_count             = src.purchase_count,
    view_count                 = src.view_count,
    conversion_rate_percentage = src.conversion_rate_percentage
WHEN NOT MATCHED THEN
  INSERT (
    event_time_ymd,
    purchase_count,
    view_count,
    conversion_rate_percentage
  )
  VALUES (
    src.event_time_ymd,
    src.purchase_count,
    src.view_count,
    src.conversion_rate_percentage
  );

-- ============================================================
-- 1-3) WEEKLY_RETENTION 분석 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.WEEKLY_RETENTION AS tgt
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
-- 1-4) MONTHLY_RETENTION 분석 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.MONTHLY_RETENTION AS tgt
USING (
  WITH monthly_cohorts AS (
    SELECT
      DATE_TRUNC('month', cohort_date)           AS cohort_month,
      month_diff,
      SUM(retained_users)                        AS total_retained_users
    FROM ECOMMERCE_DATA.STAGING_DATA.MONTHLY_RETENTION
    WHERE cohort_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
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
    'Month ' || m.month_diff                   AS month_label,
    m.total_retained_users                     AS retained_users,
    ROUND(m.total_retained_users / c.base_users, 3) AS retention_rate_pct
  FROM monthly_cohorts m
  JOIN cohort_base c
    ON m.cohort_month = c.cohort_month
) AS src
  ON tgt.cohort_month = src.cohort_month
 AND tgt.month_label  = src.month_label
WHEN MATCHED THEN
  UPDATE SET
    retained_users     = src.retained_users,
    retention_rate_pct = src.retention_rate_pct
WHEN NOT MATCHED THEN
  INSERT (
    cohort_month,
    month_label,
    retained_users,
    retention_rate_pct
  )
  VALUES (
    src.cohort_month,
    src.month_label,
    src.retained_users,
    src.retention_rate_pct
  );

-- ============================================================
-- 1-5) ACTIVE_USERS 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.ACTIVE_USERS AS tgt
USING (
  SELECT
    COALESCE(d.event_date, w.event_date, m.event_date) AS event_date,
    d.dau,
    w.wau,
    m.mau
  FROM (
    SELECT CAST(event_time AS DATE) AS event_date,
           COUNT(DISTINCT user_id)  AS dau
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY CAST(event_time AS DATE)
  ) d
  FULL OUTER JOIN (
    SELECT DATE_TRUNC('week', event_time) AS event_date,
           COUNT(DISTINCT user_id)        AS wau
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('week', event_time)
  ) w
    ON d.event_date = w.event_date
  FULL OUTER JOIN (
    SELECT DATE_TRUNC('month', event_time) AS event_date,
           COUNT(DISTINCT user_id)         AS mau
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('month', event_time)
  ) m
    ON COALESCE(d.event_date, w.event_date) = m.event_date
) AS src
  ON tgt.event_date = src.event_date
WHEN MATCHED THEN
  UPDATE SET dau = src.dau, wau = src.wau, mau = src.mau
WHEN NOT MATCHED THEN
  INSERT (event_date, dau, wau, mau)
  VALUES (src.event_date, src.dau, src.wau, src.mau);

-- ============================================================
-- 1-6) STICKINESS 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.STICKINESS AS tgt
USING (
  WITH daily AS (
    SELECT CAST(event_time AS DATE) AS event_date,
           COUNT(DISTINCT user_id)  AS daily_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY CAST(event_time AS DATE)
  ),
  monthly AS (
    SELECT DATE_TRUNC('month', event_time) AS event_month,
           COUNT(DISTINCT user_id)         AS monthly_active_users
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('month', event_time)
  ),
  daily_avg AS (
    SELECT DATE_TRUNC('month', event_date) AS event_month,
           AVG(daily_active_users)       AS avg_daily_active_users
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
-- 1-7) FUNNEL 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.FUNNEL AS tgt
USING (
  SELECT
    DATE_TRUNC('month', event_time) AS month,
    event_type,
    COUNT(*)                      AS cnt
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY DATE_TRUNC('month', event_time), event_type
) AS src
  ON tgt.month      = src.month
 AND tgt.event_type = src.event_type
WHEN MATCHED THEN
  UPDATE SET cnt = src.cnt
WHEN NOT MATCHED THEN
  INSERT (month, event_type, cnt)
  VALUES (src.month, src.event_type, src.cnt);

-- ============================================================
-- 1-8) COUNT_VISITORS 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.COUNT_VISITORS AS tgt
USING (
  SELECT
    event_time_ymd            AS event_time_ymd,
    COUNT(DISTINCT user_session) AS user_session_count
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY event_time_ymd
) AS src
  ON tgt.event_time_ymd = src.event_time_ymd
WHEN MATCHED THEN
  UPDATE SET user_session_count = src.user_session_count
WHEN NOT MATCHED THEN
  INSERT (event_time_ymd, user_session_count)
  VALUES (src.event_time_ymd, src.user_session_count);

-- ============================================================
-- 1-9) SEGMENTED_RFM 증분 Upsert (CTE 활용 RFM 계산)
-- ============================================================
-- MERGE INTO ECOMMERCE_DATA.ANALYSIS.SEGMENTED_RFM AS tgt
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

-- ============================================================
-- 2-1) PURCHASED_ORDER 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.PURCHASED_ORDER AS tgt
USING (
  SELECT
    event_time_ymd,
    SUM(price)                       AS daily_price,
    COUNT(DISTINCT user_session)     AS session_purchase_count
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_type = 'purchase'
    AND event_time_ymd BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY event_time_ymd
) AS src
  ON tgt.event_time_ymd = src.event_time_ymd
WHEN MATCHED THEN
  UPDATE SET
    daily_price             = src.daily_price,
    session_purchase_count  = src.session_purchase_count
WHEN NOT MATCHED THEN
  INSERT (event_time_ymd, daily_price, session_purchase_count)
  VALUES (src.event_time_ymd, src.daily_price, src.session_purchase_count);

-- ============================================================
-- 2-2) PRICE_DISTRIBUTION 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.PRICE_DISTRIBUTION AS tgt
USING (
  SELECT
    event_time_month,
    event_time_day_name,
    event_time_hour,
    SUM(price) AS hourly_price
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_type = 'purchase'
    AND event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  GROUP BY event_time_month, event_time_day_name, event_time_hour
) AS src
  ON   tgt.event_time_month    = src.event_time_month
    AND tgt.event_time_day_name = src.event_time_day_name
    AND tgt.event_time_hour     = src.event_time_hour
WHEN MATCHED THEN
  UPDATE SET hourly_price = src.hourly_price
WHEN NOT MATCHED THEN
  INSERT (event_time_month, event_time_day_name, event_time_hour, hourly_price)
  VALUES (src.event_time_month, src.event_time_day_name, src.event_time_hour, src.hourly_price);

-- ============================================================
-- 2-3) MONTHLY_PRICE_EVENT_TYPE 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.MONTHLY_PRICE_EVENT_TYPE AS tgt
USING (
  WITH monthly_price AS (
    SELECT
      DATE_TRUNC('month', event_time) AS month,
      SUM(price)                     AS sum_price
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_type = 'purchase'
      AND event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('month', event_time)
  ),
  monthly_event_type AS (
    SELECT
      DATE_TRUNC('month', event_time) AS month,
      event_type,
      COUNT(*)                       AS event_type_count
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY DATE_TRUNC('month', event_time), event_type
  )
  SELECT 
    met.month,
    met.event_type,
    met.event_type_count,
    mp.sum_price
  FROM monthly_event_type AS met
  LEFT JOIN monthly_price AS mp
    ON met.month = mp.month
) AS src
  ON tgt.month      = src.month
 AND tgt.event_type = src.event_type
WHEN MATCHED THEN
  UPDATE SET
    event_type_count = src.event_type_count,
    sum_price        = src.sum_price
WHEN NOT MATCHED THEN
  INSERT (month, event_type, event_type_count, sum_price)
  VALUES (src.month, src.event_type, src.event_type_count, src.sum_price);

-- ============================================================
-- 2-4) LATEST_DATE 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.LATEST_DATE AS tgt
USING (
  SELECT 
    MAX(event_time) AS latest_event_time
  FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME
  WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
) AS src
  ON TRUE
WHEN MATCHED THEN
  UPDATE SET latest_event_time = src.latest_event_time
WHEN NOT MATCHED THEN
  INSERT (latest_event_time)
  VALUES (src.latest_event_time);

-- ============================================================
-- 3-1) CATEGORY_PRICE_LV2: Category 중분류별 판매 Top 10 증분 MERGE
-- ============================================================
MERGE INTO ECOMMERCE_DATA.ANALYSIS.CATEGORY_PRICE_LV2 AS tgt
USING (
  WITH aggregated_data AS (
    SELECT
      DATE_TRUNC('month', ft.event_time)      AS month,
      ft.category_lv_2                         AS category_lv_2,
      ft.brand                                 AS brand,
      SUM(ft.price)                            AS sum_price
    FROM ECOMMERCE_DATA.STAGING_DATA.FACT_CATEGORY_PRODUCT_TIME AS ft
    WHERE ft.event_type = 'purchase'
      AND ft.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      AND ft.category_lv_2 <> 'unknown'
    GROUP BY DATE_TRUNC('month', ft.event_time), ft.category_lv_2, ft.brand
  ),
  ranked AS (
    SELECT
      month,
      category_lv_2,
      brand,
      sum_price,
      ROW_NUMBER() OVER (
        PARTITION BY month
        ORDER BY sum_price DESC
      ) AS rn
    FROM aggregated_data
  )
  SELECT
    month,
    category_lv_2,
    brand,
    sum_price
  FROM ranked
  WHERE rn <= 10
) AS src
  ON tgt.month         = src.month
 AND tgt.category_lv_2 = src.category_lv_2
 AND tgt.brand         = src.brand
WHEN MATCHED THEN
  UPDATE SET sum_price = src.sum_price
WHEN NOT MATCHED THEN
  INSERT (month, category_lv_2, brand, sum_price)
  VALUES (src.month, src.category_lv_2, src.brand, src.sum_price);
