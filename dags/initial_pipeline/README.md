# Initial Pipeline - 시작 가이드

## 개요

Initial Pipeline은 E-Commerce 행동 데이터를 처음 설정하고 로드하는 일회성 파이프라인입니다. <br>
이 파이프라인은 로컬의 Parquet 데이터를 S3에 업로드하고, 데이터를 변환한 후, Snowflake Data Warehouse에 Star Schema 기반으로 적재합니다.

---

## 사전 요구사항

### 1. 필수 소프트웨어
- Docker 및 Docker Compose 설치
- 최소 8GB RAM (권장: 16GB)
- 최소 2 CPU 코어 (권장: 4 코어)

### 2. 클라우드 서비스 계정
- **AWS 계정**: S3 버킷 생성 및 접근 권한 필요
- **Snowflake 계정**: 데이터베이스 및 웨어하우스 생성 필요

### 3. 데이터 준비
- 원본 CSV 데이터를 `data/raw_data/` 디렉토리에 배치
- Parquet 변환이 필요한 경우 `convert_to_parquet_with_spark.ipynb` 실행

---

## 설치 및 설정

### 1. 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 내용을 설정합니다:

```bash
# Airflow
AIRFLOW_UID=50000

# AWS Credentials (Airflow Connection에서도 설정 필요)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Snowflake Credentials (Airflow Connection에서 설정)
# snowflake_default connection 생성 필요
```

### 2. Docker 컨테이너 시작

프로젝트 루트 디렉토리에서 다음 명령어를 실행합니다:

```bash
# Docker Compose로 전체 스택 실행
docker-compose up -d
```

실행되는 주요 서비스:
- **postgres**: Airflow 메타데이터 DB (포트 5432)
- **redis**: Celery 메시지 브로커 (포트 6379)
- **airflow-webserver**: Airflow UI (포트 8080)
- **airflow-scheduler**: DAG 스케줄러
- **airflow-worker**: Celery 워커
- **airflow-triggerer**: 트리거 관리
- **spark-master**: Spark 마스터 노드 (포트 7077, UI: 8081)
- **spark-worker**: Spark 워커 노드

### 3. 컨테이너 상태 확인

```bash
# 모든 컨테이너가 정상 실행 중인지 확인
docker-compose ps
```

모든 서비스가 `healthy` 또는 `running` 상태인지 확인하세요.

---

## Airflow 설정

### 1. Airflow 웹 UI 접속

브라우저에서 `http://localhost:8080` 접속

- **Username**: `admin`
- **Password**: `admin`

### 2. Airflow Connections 설정

상단 메뉴에서 **Admin > Connections**로 이동하여 다음 연결들을 설정합니다:

#### AWS Connection
1. **Conn Id**: `aws_connection_id`
2. **Conn Type**: `Amazon Web Services`
3. **AWS Access Key ID**: `your_access_key`
4. **AWS Secret Access Key**: `your_secret_key`

#### Snowflake Connection
1. **Conn Id**: `snowflake_default`
2. **Conn Type**: `Snowflake`
3. **Account**: `your_snowflake_account`
4. **Database**: `ECOMMERCE_DATA`
5. **Schema**: `RAW_DATA`
6. **Login**: `your_username`
7. **Password**: `your_password`
8. **Warehouse**: `your_warehouse`
9. **Role**: `your_role`

#### Spark Connection
1. **Conn Id**: `spark_default`
2. **Conn Type**: `Spark`
3. **Host**: `spark://spark-master`
4. **Port**: `7077`

### 3. Snowflake 데이터베이스 준비

Snowflake에서 다음 명령어를 실행하여 데이터베이스와 스키마를 생성합니다:

```sql
-- 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DATA;

-- 스키마 생성
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DATA.RAW_DATA;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DATA.PROCESSED_DATA;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DATA.STAGING_DATA;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DATA.ANALYSIS;

-- Storage Integration 생성 (S3 연동)
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-account-id:role/your-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');

-- External Stage 생성
CREATE OR REPLACE STAGE ECOMMERCE_DATA.RAW_DATA.s3_stage
  URL = 's3://your-bucket-name/raw-data/'
  STORAGE_INTEGRATION = s3_integration
  FILE_FORMAT = (TYPE = PARQUET);
```

---

## 파이프라인 실행

### 1. DAG 활성화

Airflow UI에서:
1. DAGs 목록에서 `initial_pipeline` 찾기
2. 왼쪽 토글 스위치를 클릭하여 DAG 활성화
3. 오른쪽의 "Trigger DAG" 버튼 클릭하여 수동 실행

### 2. 파이프라인 작업 흐름

Initial Pipeline은 다음 순서로 실행됩니다:

#### Task 1: initial_extract
- **목적**: 로컬 Parquet 데이터를 S3 Raw 폴더에 업로드
- **입력**: `data/parquet_data/total_merged_parquet/`
- **출력**: `s3://your-bucket/raw-data/YYMMDD/`
- **실행 시간**: 약 5-10분 (데이터 크기에 따라 상이)

#### Task 2: initial_transform
- **목적**: 데이터 변환 (결측치 처리, 컬럼 분리 및 재배치)
- **입력**: `s3://your-bucket/raw-data/`
- **출력**: `s3://your-bucket/processed-data/YYMMDD/`
- **변환 작업**:
  - 결측치 처리 (imputation)
  - `category_code` 컬럼을 3개 레벨로 분리
  - `event_time`을 날짜/시간 컬럼으로 분리
  - 컬럼 순서 재배치
- **실행 시간**: 약 10-15분

#### Task 3: reset_tables
- **목적**: Snowflake RAW_DATA 스키마의 기존 테이블 초기화
- **작업**: `ECOMMERCE_EVENTS` 테이블 DROP 및 재생성
- **실행 시간**: 약 1분

#### Task 4: copy_raw_data
- **목적**: S3 Raw 데이터를 Snowflake RAW_DATA 스키마로 COPY
- **사용 SQL**: `sql/copy_into_snowflake_from_s3.sql`
- **실행 시간**: 약 5-10분

#### Task 5: create_star_schema
- **목적**: PROCESSED_DATA 스키마에 Star Schema 테이블 생성
- **생성 테이블**:
  - Fact 테이블: `FACT`
  - Dimension 테이블: `TIME_DIM`, `CATEGORY_DIM`, `PRODUCT_DIM`, `USER_DIM`
- **사용 SQL**: `sql/star_schema_for_initial_processed_data.sql`
- **실행 시간**: 약 1분

#### Task 6: insert_data_to_star_schema
- **목적**: S3 Processed 데이터를 Star Schema 테이블에 삽입
- **사용 SQL**: `sql/insert_into_initial_processed_data.sql`
- **실행 시간**: 약 10-20분

#### Task 7: create_staging_schema
- **목적**: STAGING_DATA 스키마 테이블 생성 및 데이터 삽입
- **사용 SQL**: `sql/query_for_staging_data.sql`
- **실행 시간**: 약 5-10분

#### Task 8: create_analysis_schema
- **목적**: ANALYSIS 스키마 테이블 생성 및 집계 데이터 삽입
- **사용 SQL**: `sql/query_for_analysis.sql`
- **실행 시간**: 약 5-10분

### 3. 실행 모니터링

#### Airflow UI에서 모니터링
- **Graph View**: DAG의 전체 작업 흐름 및 의존성 확인
- **Tree View**: 각 실행의 시간별 성공/실패 상태 확인
- **Task Logs**: 각 Task 클릭 후 "Log" 탭에서 상세 로그 확인

#### Spark UI에서 모니터링
- **Spark Master UI**: `http://localhost:8081`
  - 실행 중인 Application 목록
  - Worker 상태 및 리소스 사용량
- **Spark Application UI**: `http://localhost:4040` (실행 중일 때만)
  - Job, Stage, Task 단위 실행 상태
  - Executor 메모리 및 CPU 사용량

---

## 트러블슈팅

### 일반적인 문제 및 해결 방법

#### 1. Spark 연결 오류
```
Error: Connection refused to spark://spark-master:7077
```

**해결 방법**:
```bash
# Spark Master 컨테이너 상태 확인
docker-compose ps spark-master

# Spark Master 로그 확인
docker-compose logs spark-master

# 필요시 재시작
docker-compose restart spark-master
```

#### 2. S3 업로드 권한 오류
```
Error: Access Denied (403)
```

**해결 방법**:
- AWS Credentials가 올바르게 설정되었는지 확인
- S3 버킷 권한 정책 확인
- IAM 사용자에게 `s3:PutObject`, `s3:GetObject` 권한이 있는지 확인

#### 3. Snowflake 연결 오류
```
Error: 250001: Could not connect to Snowflake
```

**해결 방법**:
- Snowflake Connection의 계정 정보가 정확한지 확인
- 네트워크 방화벽에서 Snowflake 접근이 차단되지 않았는지 확인
- Snowflake 웨어하우스가 실행 중인지 확인

#### 4. 메모리 부족 오류
```
Error: OutOfMemoryError in Spark
```

**해결 방법**:
`docker-compose.yaml`에서 Spark Worker 설정 수정:

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_CORES=4        # CPU 코어 증가
    - SPARK_WORKER_MEMORY=8G      # 메모리 증가
```

변경 후 재시작:
```bash
docker-compose down
docker-compose up -d
```

#### 5. Task 실패 시 재실행
- Airflow UI에서 실패한 Task 클릭
- "Clear" 버튼 클릭하여 Task만 재실행
- 또는 "Trigger DAG" 버튼으로 전체 DAG 재실행

---

## 실행 결과 확인

### 1. S3 데이터 확인

AWS S3 콘솔 또는 AWS CLI로 확인:

```bash
# Raw 데이터 확인
aws s3 ls s3://your-bucket/raw-data/

# Processed 데이터 확인
aws s3 ls s3://your-bucket/processed-data/
```

### 2. Snowflake 데이터 확인

Snowflake에서 다음 쿼리 실행:

```sql
-- RAW_DATA 스키마 확인
SELECT COUNT(*) FROM ECOMMERCE_DATA.RAW_DATA.ECOMMERCE_EVENTS;

-- PROCESSED_DATA 스키마 확인
SELECT COUNT(*) FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT;
SELECT COUNT(*) FROM ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM;
SELECT COUNT(*) FROM ECOMMERCE_DATA.PROCESSED_DATA.CATEGORY_DIM;
SELECT COUNT(*) FROM ECOMMERCE_DATA.PROCESSED_DATA.PRODUCT_DIM;
SELECT COUNT(*) FROM ECOMMERCE_DATA.PROCESSED_DATA.USER_DIM;

-- ANALYSIS 스키마 확인
SHOW TABLES IN SCHEMA ECOMMERCE_DATA.ANALYSIS;
```

### 3. 데이터 샘플 확인

```sql
-- Fact 테이블 샘플
SELECT * FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT LIMIT 10;

-- 시간별 이벤트 분석
SELECT 
    t.event_time_ymd,
    f.event_type,
    COUNT(*) as event_count
FROM ECOMMERCE_DATA.PROCESSED_DATA.FACT f
JOIN ECOMMERCE_DATA.PROCESSED_DATA.TIME_DIM t ON f.time_id = t.time_id
GROUP BY t.event_time_ymd, f.event_type
ORDER BY t.event_time_ymd, f.event_type;
```

---

## 다음 단계

Initial Pipeline이 성공적으로 완료되면:

1. **Weekly Pipeline 설정**: 증분 데이터를 주기적으로 처리하는 파이프라인 활성화
2. **데이터 시각화**: Preset.io 또는 Tableau를 사용하여 대시보드 구축
3. **데이터 분석**: ANALYSIS 스키마의 집계 테이블을 활용하여 인사이트 도출

---

## 설정 파일 참조

### 데이터 필터링 설정
- **파일**: `dags/config/data_filtering_config.py`
- **설정 항목**: 
  - `BASE_PATH`: 로컬 데이터 경로
  - `NEW_COLUMN_ORDER`: 최종 컬럼 순서
  - `INITIAL_CONTEXT`: 초기 데이터 날짜 범위

### S3 설정
- **파일**: `dags/config/s3_config.py`
- **설정 항목**:
  - `S3_BUCKET_NAME`: S3 버킷 이름
  - `RAW_FOLDER`: Raw 데이터 폴더명
  - `PROCESSED_FOLDER`: Processed 데이터 폴더명

### Snowflake 설정
- **파일**: `dags/config/snowflake_config.py`
- **설정 항목**:
  - `SQL_DIR`: SQL 쿼리 템플릿 디렉토리

---

