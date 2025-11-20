# Snowflake 데이터 스키마 문서

## 개요

이 문서는 E-Commerce 행동 데이터를 Snowflake Data Warehouse에 저장하고 분석하기 위한 데이터 스키마를 설명합니다. <br>프로젝트는 **Star Schema** 디자인 패턴을 기반으로 하며, 3개의 주요 스키마로 구성됩니다.

---

## 스키마 구조

```
ECOMMERCE_DATA (Database)
│
├── RAW_DATA (Schema)
│   └── ECOMMERCE_EVENTS (Table)
│
├── PROCESSED_DATA (Schema)
│   ├── FACT (Fact Table)
│   ├── TIME_DIM (Dimension Table)
│   ├── CATEGORY_DIM (Dimension Table)
│   ├── PRODUCT_DIM (Dimension Table)
│   └── USER_DIM (Dimension Table)
│
├── STAGING_DATA (Schema)
│   └── [집계 및 변환 테이블들]
│
└── ANALYSIS (Schema)
    └── [분석용 집계 테이블들]
```

---

## 1. RAW_DATA Schema

### 목적
S3에서 직접 복사된 원본 데이터를 저장하는 스키마입니다. 데이터의 원형을 보존하며, 추후 재처리나 감사(Audit)가 필요할 때 사용됩니다.

### 테이블: ECOMMERCE_EVENTS

원본 e-commerce 이벤트 데이터를 저장합니다.

#### 테이블 정의

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.RAW_DATA.ECOMMERCE_EVENTS (
    event_time TIMESTAMP_NTZ,
    event_type VARCHAR,
    product_id VARCHAR,
    category_id VARCHAR,
    category_code VARCHAR,
    brand VARCHAR,
    price FLOAT,
    user_id VARCHAR,
    user_session VARCHAR
);
```

#### 컬럼 설명

| 컬럼명 | 데이터 타입 | 설명 | 예시 |
|--------|-------------|------|------|
| `event_time` | TIMESTAMP_NTZ | 이벤트 발생 시간 (타임존 없음) | 2019-10-01 00:00:00 |
| `event_type` | VARCHAR | 이벤트 유형 | view, cart, purchase |
| `product_id` | VARCHAR | 제품 고유 식별자 | 1004767 |
| `category_id` | VARCHAR | 카테고리 고유 식별자 | 2053013555631882655 |
| `category_code` | VARCHAR | 카테고리 계층 구조 | electronics.smartphone |
| `brand` | VARCHAR | 브랜드 이름 | samsung, apple |
| `price` | FLOAT | 제품 가격 | 489.07 |
| `user_id` | VARCHAR | 사용자 고유 식별자 | 520088904 |
| `user_session` | VARCHAR | 사용자 세션 ID | 4d3b30da-a5e4-49df-b1a8-ba5943f1dd33 |

#### 데이터 로딩

**SQL 파일**: `copy_into_snowflake_from_s3.sql`

```sql
-- S3에서 RAW_DATA로 데이터 COPY
COPY INTO ECOMMERCE_DATA.RAW_DATA.ECOMMERCE_EVENTS
FROM @s3_stage
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
ON_ERROR = CONTINUE;
```

---

## 2. PROCESSED_DATA Schema

### 목적
Star Schema 기반으로 정규화된 데이터 구조를 제공합니다. Fact 테이블과 여러 Dimension 테이블로 구성되어 있으며, 효율적인 분석 쿼리를 위해 설계되었습니다.

### Star Schema 설계

Star Schema는 다음과 같은 장점을 제공합니다:
- **쿼리 성능 최적화**: JOIN 연산이 단순하여 빠른 조회 가능
- **유지보수 용이성**: 차원 테이블의 독립적 관리
- **확장성**: 새로운 차원 추가 용이
- **비즈니스 이해도**: 직관적인 구조로 비즈니스 로직 이해 쉬움

### 2.1 Fact Table: FACT

핵심 이벤트 정보와 각 Dimension 테이블에 대한 외래 키를 포함합니다.

#### 테이블 정의

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.FACT (
    event_id INTEGER AUTOINCREMENT PRIMARY KEY,
    time_id INTEGER,
    category_id VARCHAR,
    product_id VARCHAR,
    user_id VARCHAR,
    user_session VARCHAR,
    event_type VARCHAR
);
```

#### 컬럼 설명

| 컬럼명 | 데이터 타입 | 설명 | 키 타입 |
|--------|-------------|------|---------|
| `event_id` | INTEGER | 이벤트 고유 ID (자동 증가) | Primary Key |
| `time_id` | INTEGER | 시간 차원 외래 키 | Foreign Key → TIME_DIM |
| `category_id` | VARCHAR | 카테고리 차원 외래 키 | Foreign Key → CATEGORY_DIM |
| `product_id` | VARCHAR | 제품 차원 외래 키 | Foreign Key → PRODUCT_DIM |
| `user_id` | VARCHAR | 사용자 차원 외래 키 | Foreign Key → USER_DIM |
| `user_session` | VARCHAR | 사용자 세션 ID | - |
| `event_type` | VARCHAR | 이벤트 타입 (view, cart, purchase) | - |

### 2.2 Dimension Tables

#### TIME_DIM (시간 차원)

시간 관련 모든 속성을 저장하여 시간 기반 분석을 용이하게 합니다.

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM (
    time_id INTEGER AUTOINCREMENT PRIMARY KEY,
    event_time TIMESTAMP_NTZ,
    event_time_ymd DATE,
    event_time_hms TIME,
    event_time_month VARCHAR,
    event_time_day VARCHAR,
    event_time_hour VARCHAR,
    event_time_day_name VARCHAR
);
```

**컬럼 설명**:

| 컬럼명 | 데이터 타입 | 설명 | 예시 |
|--------|-------------|------|------|
| `time_id` | INTEGER | 시간 차원 고유 ID | 1, 2, 3... |
| `event_time` | TIMESTAMP_NTZ | 원본 이벤트 시간 | 2019-10-01 00:00:00 |
| `event_time_ymd` | DATE | 연-월-일 | 2019-10-01 |
| `event_time_hms` | TIME | 시:분:초 | 14:35:22 |
| `event_time_month` | VARCHAR | 월 | 10 |
| `event_time_day` | VARCHAR | 일 | 01 |
| `event_time_hour` | VARCHAR | 시 | 14 |
| `event_time_day_name` | VARCHAR | 요일명 | Tuesday |

**사용 예시**:
```sql
-- 시간대별 이벤트 수 분석
SELECT 
    event_time_hour,
    COUNT(*) as event_count
FROM FACT f
JOIN TIME_DIM t ON f.time_id = t.time_id
GROUP BY event_time_hour
ORDER BY event_time_hour;
```

---

#### CATEGORY_DIM (카테고리 차원)

제품 카테고리의 계층 구조를 저장합니다.

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM (
    category_id VARCHAR PRIMARY KEY,
    category_code VARCHAR,
    category_lv_1 VARCHAR,
    category_lv_2 VARCHAR,
    category_lv_3 VARCHAR
);
```

**컬럼 설명**:

| 컬럼명 | 데이터 타입 | 설명 | 예시 |
|--------|-------------|------|------|
| `category_id` | VARCHAR | 카테고리 고유 ID | 2053013555631882655 |
| `category_code` | VARCHAR | 원본 카테고리 코드 | electronics.smartphone |
| `category_lv_1` | VARCHAR | 1단계 카테고리 | electronics |
| `category_lv_2` | VARCHAR | 2단계 카테고리 | smartphone |
| `category_lv_3` | VARCHAR | 3단계 카테고리 | NULL (있는 경우만) |

**카테고리 계층 구조**:
```
electronics (Lv1)
├── smartphone (Lv2)
├── tablet (Lv2)
└── audio (Lv2)
    └── headphone (Lv3)
```

**사용 예시**:
```sql
-- 1단계 카테고리별 매출 분석
SELECT 
    c.category_lv_1,
    COUNT(DISTINCT f.product_id) as product_count,
    SUM(p.price) as total_revenue
FROM FACT f
JOIN CATEGORY_DIM c ON f.category_id = c.category_id
JOIN PRODUCT_DIM p ON f.product_id = p.product_id
WHERE f.event_type = 'purchase'
GROUP BY c.category_lv_1
ORDER BY total_revenue DESC;
```

---

#### PRODUCT_DIM (제품 차원)

제품의 속성 정보를 저장합니다.

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM (
    product_id VARCHAR PRIMARY KEY,
    brand VARCHAR,
    price FLOAT
);
```

**컬럼 설명**:

| 컬럼명 | 데이터 타입 | 설명 | 예시 |
|--------|-------------|------|------|
| `product_id` | VARCHAR | 제품 고유 ID | 1004767 |
| `brand` | VARCHAR | 브랜드명 | samsung, apple |
| `price` | FLOAT | 제품 가격 | 489.07 |

**사용 예시**:
```sql
-- 브랜드별 평균 가격 및 제품 수
SELECT 
    brand,
    COUNT(DISTINCT product_id) as product_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM PRODUCT_DIM
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY avg_price DESC;
```

---

#### USER_DIM (사용자 차원)

사용자 정보를 저장합니다.

```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.PROCESSED_DATA.USER_DIM (
    user_id VARCHAR PRIMARY KEY,
    user_session VARCHAR
);
```

**컬럼 설명**:

| 컬럼명 | 데이터 타입 | 설명 | 예시 |
|--------|-------------|------|------|
| `user_id` | VARCHAR | 사용자 고유 ID | 520088904 |
| `user_session` | VARCHAR | 사용자 세션 ID | 4d3b30da-a5e4-49df-b1a8-ba5943f1dd33 |

**사용 예시**:
```sql
-- 사용자별 세션 수 및 이벤트 수
SELECT 
    u.user_id,
    COUNT(DISTINCT u.user_session) as session_count,
    COUNT(f.event_id) as total_events
FROM USER_DIM u
JOIN FACT f ON u.user_id = f.user_id
GROUP BY u.user_id
ORDER BY total_events DESC
LIMIT 10;
```

---

## 3. STAGING_DATA Schema

### 목적
분석을 위한 중간 집계 테이블들을 저장합니다. RAW_DATA나 PROCESSED_DATA를 기반으로 생성되며, 복잡한 비즈니스 로직이 적용된 데이터를 포함합니다.

### 특징
- 반복적으로 사용되는 집계 쿼리의 결과를 저장
- 분석 쿼리의 성능 향상
- 비즈니스 로직의 일관성 유지

### 주요 테이블 예시

```sql
-- 일별 이벤트 집계
CREATE OR REPLACE TABLE ECOMMERCE_DATA.STAGING_DATA.DAILY_EVENT_SUMMARY AS
SELECT 
    t.event_time_ymd,
    f.event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT f.user_id) as unique_users,
    COUNT(DISTINCT f.product_id) as unique_products
FROM PROCESSED_DATA.FACT f
JOIN PROCESSED_DATA.TIME_DIM t ON f.time_id = t.time_id
GROUP BY t.event_time_ymd, f.event_type;
```

---

## 4. ANALYSIS Schema

### 목적
최종 분석 및 리포팅을 위한 집계 테이블들을 저장합니다. Preset.io 또는 Tableau와 같은 BI 도구에서 직접 사용됩니다.

### 특징
- 비즈니스 질문에 대한 직접적인 답변 제공
- 고도로 집계된 데이터
- 시각화 도구에 최적화된 구조

### 주요 분석 테이블 예시

#### 전환율 분석
```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.CONVERSION_FUNNEL AS
SELECT 
    t.event_time_ymd,
    SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as views,
    SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) as carts,
    SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    ROUND(carts / NULLIF(views, 0) * 100, 2) as view_to_cart_rate,
    ROUND(purchases / NULLIF(carts, 0) * 100, 2) as cart_to_purchase_rate
FROM PROCESSED_DATA.FACT f
JOIN PROCESSED_DATA.TIME_DIM t ON f.time_id = t.time_id
GROUP BY t.event_time_ymd;
```

#### 카테고리별 매출 분석
```sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.CATEGORY_REVENUE AS
SELECT 
    c.category_lv_1,
    c.category_lv_2,
    COUNT(DISTINCT f.product_id) as product_count,
    COUNT(DISTINCT f.user_id) as customer_count,
    SUM(p.price) as total_revenue,
    AVG(p.price) as avg_price
FROM PROCESSED_DATA.FACT f
JOIN PROCESSED_DATA.CATEGORY_DIM c ON f.category_id = c.category_id
JOIN PROCESSED_DATA.PRODUCT_DIM p ON f.product_id = p.product_id
WHERE f.event_type = 'purchase'
GROUP BY c.category_lv_1, c.category_lv_2;
```

---

## SQL 쿼리 템플릿

이 프로젝트는 **Jinja2** 템플릿 엔진을 사용하여 동적 SQL 쿼리를 생성합니다.

### 템플릿 파일 목록

#### Initial Pipeline용
- `reset_tables_for_initial_raw_data.sql`: RAW_DATA 스키마 초기화
- `copy_into_snowflake_from_s3.sql`: S3 → RAW_DATA COPY
- `star_schema_for_initial_processed_data.sql`: Star Schema 생성
- `insert_into_initial_processed_data.sql`: PROCESSED_DATA 삽입
- `query_for_staging_data.sql`: STAGING_DATA 생성
- `query_for_analysis.sql`: ANALYSIS 테이블 생성

#### Weekly Pipeline용
- `weekly_copy_into_snowflake_from_s3.sql`: 증분 데이터 COPY
- `weekly_insert_into_initial_processed_data.sql`: 증분 데이터 삽입
- `weekly_query_for_staging_data.sql`: STAGING_DATA 업데이트
- `weekly_query_for_analysis.sql`: ANALYSIS 업데이트

### Jinja2 템플릿 예시

```sql
-- query_for_analysis.sql
CREATE OR REPLACE TABLE ECOMMERCE_DATA.ANALYSIS.DAILY_SUMMARY AS
SELECT 
    t.event_time_ymd,
    COUNT(*) as total_events,
    COUNT(DISTINCT f.user_id) as unique_users
FROM PROCESSED_DATA.FACT f
JOIN PROCESSED_DATA.TIME_DIM t ON f.time_id = t.time_id
WHERE t.event_time_ymd BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY t.event_time_ymd;
```

**Python에서 사용**:
```python
from jinja2 import Template

template = Template(sql_query)
rendered_sql = template.render(
    start_date='2019-10-01',
    end_date='2019-11-25'
)
```

---

## 데이터 모델링 모범 사례

### 1. 정규화 vs 비정규화
- **PROCESSED_DATA**: 정규화된 Star Schema (쿼리 성능 최적화)
- **ANALYSIS**: 비정규화된 플랫 테이블 (BI 도구 최적화)

### 2. Slowly Changing Dimensions (SCD)
현재는 Type 1 SCD (덮어쓰기)를 사용하지만, 향후 Type 2 SCD (이력 관리) 적용 고려:

```sql
-- Type 2 SCD 예시
CREATE TABLE PRODUCT_DIM (
    product_key INTEGER AUTOINCREMENT,
    product_id VARCHAR,
    brand VARCHAR,
    price FLOAT,
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN
);
```

### 3. 파티셔닝 전략
대용량 데이터 처리를 위해 날짜 기반 파티셔닝 권장:

```sql
-- Snowflake 자동 클러스터링
ALTER TABLE FACT CLUSTER BY (time_id);
```

---

## 성능 최적화

### 1. 인덱싱
```sql
-- TIME_DIM에 인덱스 생성 (날짜 검색 최적화)
CREATE INDEX idx_event_time_ymd ON TIME_DIM(event_time_ymd);

-- FACT 테이블에 복합 인덱스
CREATE INDEX idx_fact_composite ON FACT(time_id, event_type);
```

### 2. 머티리얼라이즈드 뷰
```sql
-- 자주 사용되는 집계 쿼리를 뷰로 생성
CREATE MATERIALIZED VIEW mv_daily_summary AS
SELECT 
    t.event_time_ymd,
    f.event_type,
    COUNT(*) as event_count
FROM FACT f
JOIN TIME_DIM t ON f.time_id = t.time_id
GROUP BY t.event_time_ymd, f.event_type;
```

### 3. 통계 정보 업데이트
```sql
-- 쿼리 최적화를 위한 통계 수집
ANALYZE TABLE FACT;
ANALYZE TABLE TIME_DIM;
```

---

## 데이터 품질 관리

### 1. 데이터 검증 쿼리

```sql
-- NULL 값 확인
SELECT 
    COUNT(*) - COUNT(event_time) as null_event_time,
    COUNT(*) - COUNT(user_id) as null_user_id,
    COUNT(*) - COUNT(product_id) as null_product_id
FROM RAW_DATA.ECOMMERCE_EVENTS;

-- 중복 확인
SELECT 
    user_id, 
    event_time, 
    product_id, 
    COUNT(*) as duplicate_count
FROM RAW_DATA.ECOMMERCE_EVENTS
GROUP BY user_id, event_time, product_id
HAVING COUNT(*) > 1;

-- 데이터 범위 확인
SELECT 
    MIN(event_time) as min_date,
    MAX(event_time) as max_date,
    COUNT(*) as total_records
FROM RAW_DATA.ECOMMERCE_EVENTS;
```

### 2. 참조 무결성 확인

```sql
-- FACT 테이블의 외래 키 검증
SELECT COUNT(*) as orphan_records
FROM FACT f
LEFT JOIN TIME_DIM t ON f.time_id = t.time_id
WHERE t.time_id IS NULL;
```

