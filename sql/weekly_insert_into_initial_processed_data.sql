-- =============================================
-- 1) TIME_DIM Upsert
--    • RAW_DATA 테이블에서 날짜 범위만 직접 조회
-- =============================================
MERGE INTO ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS tgt
USING (
  SELECT DISTINCT
    r.event_time,
    r.event_time_ymd,
    r.event_time_hms,
    r.event_time_month,
    r.event_time_day,
    r.event_time_hour,
    r.event_time_day_name
  FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
  WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'  -- 날짜 필터링[^1]
) AS src
  ON   tgt.event_time          = src.event_time
    AND tgt.event_time_ymd      = src.event_time_ymd
    AND tgt.event_time_hms      = src.event_time_hms
    AND tgt.event_time_month    = src.event_time_month
    AND tgt.event_time_day      = src.event_time_day
    AND tgt.event_time_hour     = src.event_time_hour
    AND tgt.event_time_day_name = src.event_time_day_name
WHEN NOT MATCHED THEN
  INSERT (
    event_time, event_time_ymd, event_time_hms,
    event_time_month, event_time_day, event_time_hour, event_time_day_name
  )
  VALUES (
    src.event_time, src.event_time_ymd, src.event_time_hms,
    src.event_time_month, src.event_time_day, src.event_time_hour, src.event_time_day_name
  );

-- =============================================
-- 2) CATEGORY_DIM Upsert
--    • RAW_DATA 테이블에서 날짜 범위에 해당하는 카테고리 정보만 조회
-- =============================================
MERGE INTO ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM AS tgt
USING (
  SELECT DISTINCT
    r.category_id,
    r.category_code,
    r.category_lv_1,
    r.category_lv_2,
    r.category_lv_3
  FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
  WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
) AS src
  ON tgt.category_id = src.category_id
WHEN NOT MATCHED THEN
  INSERT (category_id, category_code, category_lv_1, category_lv_2, category_lv_3)
  VALUES (src.category_id, src.category_code, src.category_lv_1, src.category_lv_2, src.category_lv_3);

-- =============================================
-- 3) PRODUCT_DIM Upsert
--    • Brand 및 Price 최빈값(mode) 기준으로 Upsert
-- =============================================
MERGE INTO ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM AS tgt
USING (
  WITH brand_frequency AS (
    SELECT
      r.product_id,
      r.brand,
      COUNT(*) AS brand_frequency,
      MAX(r.event_time) AS latest_brand_time
    FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
    WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY r.product_id, r.brand
  ),
  mode_brand AS (
    SELECT
      product_id,
      brand,
      ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY brand_frequency DESC, latest_brand_time DESC
      ) AS rn
    FROM brand_frequency
  ),
  price_frequency AS (
    SELECT
      r.product_id,
      r.price,
      COUNT(*) AS price_frequency,
      MAX(r.event_time) AS latest_price_time
    FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
    WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY r.product_id, r.price
  ),
  mode_price AS (
    SELECT
      product_id,
      price,
      ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY price_frequency DESC, latest_price_time DESC
      ) AS rn
    FROM price_frequency
  )
  SELECT 
    b.product_id,
    b.brand,
    p.price
  FROM mode_brand b
  JOIN mode_price p 
    ON b.product_id = p.product_id
  WHERE b.rn = 1 AND p.rn = 1
) AS src
  ON tgt.product_id = src.product_id
WHEN NOT MATCHED THEN
  INSERT (product_id, brand, price)
  VALUES (src.product_id, src.brand, src.price);

-- =============================================
-- 4) USER_DIM Upsert
--    • 사용자 정보(세션) 조회
-- =============================================
MERGE INTO ECOMMERCE_DATA.PROCESSED_DATA.USER_DIM AS tgt
USING (
  SELECT DISTINCT
    r.user_id,
    r.user_session
  FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
  WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
) AS src
  ON tgt.user_id = src.user_id
WHEN NOT MATCHED THEN
  INSERT (user_id, user_session)
  VALUES (src.user_id, src.user_session);

-- =============================================
-- 5) FACT Upsert
--    • TIME_DIM과 조인하여 최종 Fact 적재
-- =============================================
MERGE INTO ECOMMERCE_DATA.PROCESSED_DATA.FACT AS tgt
USING (
  SELECT
    t.time_id,
    r.category_id,
    r.product_id,
    r.user_id,
    r.user_session,
    r.event_type
  FROM ECOMMERCE_DATA.RAW_DATA.RAW_DATA AS r
  JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM AS t
    ON   r.event_time          = t.event_time
     AND r.event_time_ymd      = t.event_time_ymd
     AND r.event_time_hms      = t.event_time_hms
     AND r.event_time_month    = t.event_time_month
     AND r.event_time_day      = t.event_time_day
     AND r.event_time_hour     = t.event_time_hour
     AND r.event_time_day_name = t.event_time_day_name
  WHERE r.event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
) AS src
  ON   tgt.time_id      = src.time_id
    AND tgt.category_id  = src.category_id
    AND tgt.product_id   = src.product_id
    AND tgt.user_id      = src.user_id
    AND tgt.user_session = src.user_session
    AND tgt.event_type   = src.event_type
WHEN NOT MATCHED THEN
  INSERT (time_id, category_id, product_id, user_id, user_session, event_type)
  VALUES (src.time_id, src.category_id, src.product_id, src.user_id, src.user_session, src.event_type);
