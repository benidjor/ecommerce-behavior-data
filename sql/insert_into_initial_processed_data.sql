/* 1. Raw Data 테이블의 통합 데이터셋을 분리하여 Processed Data 스키마의 테이블에 각각 적재 */

/* TIME_DIM: 실제 시간 관련 값은 속성 (Attribute)으로 저장, time_id를 Primary Key로 사용 */
INSERT INTO PROCESSED_DATA.TIME_DIM (event_time, event_time_ymd, event_time_hms, event_time_month, event_time_day, event_time_hour, event_time_day_name)
SELECT DISTINCT event_time, event_time_ymd, event_time_hms, event_time_month, event_time_day, event_time_hour, event_time_day_name
FROM raw_data.raw_data
WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}';


/* CATEGORY_DIM: 제품의 카테고리 정보에 대한 상세 속성을 포함 */
INSERT INTO PROCESSED_DATA.CATEGORY_DIM (category_id, category_code, category_lv_1, category_lv_2, category_lv_3)
SELECT DISTINCT category_id, category_code, category_lv_1, category_lv_2, category_lv_3
FROM raw_data.raw_data
WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}';


/* PRODUCT_DIM: 제품 관련 정보를 관리하며, product_id를 Primary Key로 사용 
- brand_frequency CTE: 각 product_id에 대해 브랜드(brand)의 발생 빈도(brand_frequency)를 계산하고, 해당 브랜드의 최신 event_time을 구한다 (빈도 중복 시 최신 데이터에 우선 순위)
- mode_brand CTE: 각 product_id 그룹 내에서 브랜드의 빈도와 최신 event_time을 기준으로 순위 파악
- price_frequency CTE: 원본 데이터(raw_data.raw_data)에서 각 제품(product_id)별, 브랜드(brand), 그리고 가격(price)의 발생 빈도와 최신 적용 시점(event_time)을 집계
- mode_price CTE: price_frequency에서 집계된 데이터를 기반으로, 각 제품과 브랜드 그룹별로 대표 가격(최빈값)을 결정하기 위한 순위 파악 */
INSERT INTO PROCESSED_DATA.PRODUCT_DIM (product_id, brand, price)
WITH brand_frequency AS (
    SELECT
        product_id,
        brand,
        COUNT(*) AS brand_frequency,
        MAX(event_time) AS latest_brand_time
    FROM raw_data.raw_data
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY product_id, brand
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
        product_id,
        price,
        COUNT(*) AS price_frequency,
        MAX(event_time) AS latest_price_time
    FROM raw_data.raw_data
    WHERE event_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY product_id, price
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
WHERE b.rn = 1 
  AND p.rn = 1;

/* PRODUCT_DIM: 제품 관련 정보를 관리하며, product_id를 Primary Key로 사용 
- brand_frequency CTE: 각 product_id에 대해 브랜드(brand)의 발생 빈도(brand_frequency)를 계산하고, 해당 브랜드의 최신 event_time을 구한다 (빈도 중복 시 최신 데이터에 우선 순위)
- mode_brand CTE: 각 product_id 그룹 내에서 브랜드의 빈도와 최신 event_time을 기준으로 순위 파악
- price_frequency CTE: 원본 데이터(raw_data.raw_data)에서 각 제품(product_id)별, 브랜드(brand), 그리고 가격(price)의 발생 빈도와 최신 적용 시점(event_time)을 집계
- mode_price CTE: price_frequency에서 집계된 데이터를 기반으로, 각 제품과 브랜드 그룹별로 대표 가격(최빈값)을 결정하기 위한 순위 파악 */
INSERT INTO PROCESSED_DATA.PRODUCT_DIM (product_id, brand, price)
WITH brand_frequency AS (
    SELECT
        product_id,
        brand,
        COUNT(*) AS brand_frequency,
        MAX(event_time) AS latest_brand_time
    FROM raw_data.raw_data
    GROUP BY product_id, brand
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
        product_id,
        price,
        COUNT(*) AS price_frequency,
        MAX(event_time) AS latest_price_time
    FROM raw_data.raw_data
    GROUP BY product_id, price
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
WHERE b.rn = 1 
  AND p.rn = 1;


/* USER_DIM: 사용자 정보와 관련된 속성을 저장하며, user_id를 Primary Key로 사용*/
INSERT INTO PROCESSED_DATA.USER_DIM (user_id, user_session)
SELECT DISTINCT user_id, user_session
FROM raw_data.raw_data;

/* FACT Table: 핵심 이벤트 정보 (event_type, event_time, product_id, user_id 등)만을 포함 */
INSERT INTO PROCESSED_DATA.FACT (time_id, category_id, product_id, user_id, user_session, event_type)
SELECT t.time_id,
       r.category_id,
       r.product_id,
       r.user_id,
       r.user_session,
       r.event_type
FROM raw_data.raw_data AS r
INNER JOIN PROCESSED_DATA.TIME_DIM AS t
  ON r.event_time = t.event_time
  AND r.event_time_ymd = t.event_time_ymd
  AND r.event_time_hms = t.event_time_hms
  AND r.event_time_month = t.event_time_month
  AND r.event_time_day = t.event_time_day
  AND r.event_time_hour = t.event_time_hour
  AND r.event_time_day_name = t.event_time_day_name;
