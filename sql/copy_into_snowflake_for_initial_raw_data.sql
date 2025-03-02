-- Raw data 테이블에 데이터 적재
COPY INTO ECOMMERCE_DATA.RAW_DATA.RAW_DATA
FROM @ECOMMERCE_DATA.RAW_DATA.S3_ECOMMERCE_STAGE
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;