# E-Commerce Behavior Data ETL Pipeline

## 프로젝트 개요

### 배경 및 데이터 소개
- 데이터가 주기적으로 업데이트되는 환경을 가정, 과거 시점의 데이터를 현재 실행한다고 가정하고 시간의 흐름에 따라 처리
- Kaggle eCommerce behavior data from multi category store 데이터 [출처](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

### 목표
- 이커머스 데이터 기반, 데이터 엔지니어링 파이프라인 구축
     - Airflow를 통한 1주 단위 ETL, ELT 자동화
     - 기존 데이터 (**Initial Pipeline, 2019-10-01 ~ 2019-11-25**)
     - 일주일 단위로 새롭게 적재된다고 가정한 데이터 (**Weekly Batch Pipeline, 2019-11-26 ~ 2020-04-30**)
- Dashboard 업데이트를 통한 트렌드 분석 및 인사이트 도출

### 주요 특징

- **자동화된 ETL, ELT 파이프라인**: Initial 및 Weekly 파이프라인을 통한 체계적인 데이터 처리
- **분산 처리**: Apache Spark를 활용한 대용량 데이터 처리
- **클라우드 저장소**: AWS S3를 활용한 데이터 레이크 구축
- **데이터 웨어하우스**: Snowflake에서 Star Schema 기반 분석 환경 구축
- **컨테이너화**: Docker Compose를 통한 간편한 배포 및 관리
- **데이터 분석**: Staging 및 Analysis 스키마를 통한 다차원 분석
- **데이터 시각화**: Apache Superset (Preset.io)를 통한 대시보드 및 차트 시각화

---

## 아키텍처
![아키텍처](https://i.imgur.com/xcxGNU3.png)



---

## 기술 스택

| 카테고리 | 기술 |
|---------|------|
| **오케스트레이션** | Apache Airflow 2.9.1 |
| **분산 처리** | Apache Spark 3.5.5 |
| **데이터 저장소** | AWS S3, Snowflake Data Warehouse |
| **데이터 시각화** | Preset.io (Apache Superset) |
| **컨테이너화** | Docker, Docker Compose |
| **언어** | Python 3.x, SQL |
| **데이터베이스** | PostgreSQL 13 (Airflow Metadata) |

---

## 프로젝트 구조

```
ecommerce-behavior-data/
│
├── dags/                                # Airflow DAG 디렉토리
│   ├── initial_pipeline/                # 초기 데이터 로드 파이프라인
│   │   ├── dag.py                       # Initial Pipeline DAG 정의
│   │   ├── extract.py                   # 데이터 추출 (Local → S3)
│   │   ├── transform.py                 # 데이터 변환 (결측치 처리, 컬럼 재배치)
│   │   └── load.py                      # 데이터 로드 (S3 → Snowflake)
│   │
│   ├── weekly_pipeline/                 # 주간 스케줄 파이프라인
│   │   ├── dag.py                       # Weekly Pipeline DAG 정의
│   │   ├── extract.py                   # 증분 데이터 추출
│   │   ├── transform.py                 # 데이터 변환
│   │   └── load.py                      # 증분 데이터 로드
│   │
│   ├── common_utils/                    # 공통 유틸리티 모듈
│   │   ├── spark_utils.py               # Spark 세션 생성 및 관리
│   │   ├── s3_utils.py                  # S3 업로드/다운로드 로직
│   │   ├── snowflake_utils.py           # Snowflake 연결 및 SQL 실행
│   │   ├── date_utils.py                # 날짜 처리 및 필터링
│   │   ├── transform_utils.py           # 데이터 변환 로직
│   │   └── variable_utils.py            # Airflow Variable 관리
│   │
│   └── config/                          # 설정 파일
│       ├── data_filtering_config.py     # 데이터 필터링 설정
│       ├── s3_config.py                 # S3 버킷 및 경로 설정
│       └── snowflake_config.py          # Snowflake 연결 설정
│
├── sql/                                 # SQL 쿼리 템플릿 (Jinja2)
│
├── logs/                                # Airflow 로그 디렉토리
├── plugins/                             # Airflow 플러그인
├── config/                              # Airflow 설정 파일
├── spark/                               # Spark 설정 디렉토리
│
├── docker-compose.yaml                  # Docker Compose 설정
├── Dockerfile.airflow                   # Airflow 컨테이너 이미지
├── Dockerfile.spark                     # Spark 컨테이너 이미지
│
├── convert_to_parquet_with_spark.ipynb  # CSV → Parquet 변환 노트북
├── read_parquet.ipynb                   # Parquet 데이터 검증 노트북
└── README.md                            # 프로젝트 문서
```

---

## 시작하기

파이프라인을 설정하고 실행하는 방법에 대한 상세한 가이드는 아래 문서를 참조바랍니다:

**[Initial Pipeline 시작 가이드](dags/initial_pipeline/README.md)**

### 빠른 시작

1. **환경 변수 설정**: `.env` 파일 생성
2. **Docker 실행**: `docker-compose up -d`
3. **Airflow UI 접속**: `http://localhost:8080`
4. **Connections 설정**: AWS, Snowflake, Spark 연결 구성
5. **파이프라인 실행**: `initial_pipeline`/ `weekly_pipeline` DAG 활성화 및 트리거


---

## 데이터 스키마

Snowflake Data Warehouse의 스키마 구조 및 테이블 정의에 대한 상세한 문서는 아래를 참조바랍니다:

**[Snowflake 데이터 스키마 문서](sql/README.md)**

### 스키마 개요

프로젝트는 Star Schema 기반의 3개 주요 스키마로 구성됩니다:

1. **RAW_DATA**: S3에서 복사된 원본 데이터 (최소한의 전처리)
2. **PROCESSED_DATA**: Star Schema (Fact + Dimension Tables)
3. **STAGING_DATA & ANALYSIS**: 집계 및 분석용 테이블


### ERD
![Snowflake ERD](https://i.imgur.com/VnvoQMm.png)

---

## 데이터 시각화 대시보드

Preset.io(Apache Superset)를 활용하여 Snowflake의 ANALYSIS 스키마 데이터를 기반으로 인터랙티브한 대시보드를 구축했습니다.

### 1. 고객 행동 분석 대시보드

![고객 행동 분석 대시보드](https://i.imgur.com/12NFccB.jpeg)

**주요 지표 및 차트**:

- 핵심 KPI 카드
- 월별 Event Type 분포 (막대 차트)
- 월별 대비 CVR 변화 (히트맵)
- RFM Segment 분석 (도넛 차트)
- DAU & WAU 추이 (이중 라인 차트)
- MAU & Stickiness 비율 (이중 축 차트)
- 주차별 리텐션 (라인 차트)
- 고객 생애 가치 분석 (퍼널 차트)

### 2. 매출 분석 대시보드

![매출 분석 대시보드](https://i.imgur.com/Pga7yWt.jpeg)

**주요 지표 및 차트**:

- 월별 총 결제 금액 (막대 차트)
- 월별 결제 건수 히트맵 (달력 히트맵)
- 카테고리별 중복도 매출 TOP 7 (도넛 차트)
- 브랜드별 매출 TOP 7 (트리맵)
- 시간대별 결제 건수 분포 (에리어 차트)
- 요일별 및 시간대별 결제 건수 히트맵
- 카테고리의 총결제 및 판매량 제품 비중 (스트림그래프)

### 대시보드 특징

- **실시간 데이터 연동**: Snowflake ANALYSIS 스키마와 직접 연결되어 최신 데이터 반영
- **인터랙티브 필터링**: 날짜 범위, 카테고리, 브랜드 등 다양한 필터 옵션 제공
- **다차원 분석**: RFM 세그먼트, 코호트, 전환율 등 복합적인 비즈니스 지표 분석
- **직관적인 시각화**: 다양한 차트 타입(막대, 라인, 도넛, 히트맵, 퍼널 등)을 활용한 효과적인 데이터 표현
- **반응형 레이아웃**: 다양한 화면 크기에 최적화된 대시보드 구성

---

## 주요 기능

### 1. 자동화된 데이터 증분 처리
- Weekly Pipeline은 Airflow Variable을 활용하여 이미 처리된 데이터를 건너뛰고 새로운 데이터만 처리
- 날짜 필터링을 통한 S3 경로 존재 여부를 확인하여 중복 업로드 방지 (Upsert 구현)

### 2. 분산 데이터 처리
- Apache Spark를 활용하여 대용량 데이터를 효율적으로 처리

### 3. 데이터 품질 관리
- 결측치 처리 (imputation)
- 데이터 타입 변환 및 유효성 검증
- 컬럼 재배치 및 표준화

### 4. 모듈화 및 재사용성
- 공통 유틸리티 함수를 `common_utils/`에 분리하여 코드 재사용성 향상
- 설정 파일(`config/`)을 통한 중앙 집중식 구성 관리

### 5. SQL 템플릿 엔진 (Jinja2)
- SQL 쿼리에 변수 및 조건문을 사용하여 동적 쿼리 생성
- 날짜 범위, 스키마 이름 등을 파라미터로 전달하여 재사용 가능

---

## 모니터링 및 로깅

### Airflow UI
- **DAG 실행 상태**: `http://localhost:8080`
- 각 Task의 로그 확인 가능
- Task 재실행 및 스케줄 관리

### Spark UI
- **Spark Master UI**: `http://localhost:8081`
- Job, Stage, Task 단위의 실행 상태 확인
- Executor 리소스 사용량 모니터링
- **Spark Application UI**: `http://localhost:4040` (실행 중일 때만)

### 로그 파일
- Airflow 로그: `logs/` 디렉토리
- DAG 및 Task별로 구분된 로그 파일

