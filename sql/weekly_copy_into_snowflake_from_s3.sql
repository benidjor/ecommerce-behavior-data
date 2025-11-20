-- =============================================
-- S3 → RAW_DATA 증분 COPY
--    • 지정 파티션(예: processed-data/{{ s3_prefix_filter }})만 로드
-- =============================================
COPY INTO ECOMMERCE_DATA.RAW_DATA.RAW_DATA
FROM @ECOMMERCE_DATA.RAW_DATA.S3_ECOMMERCE_STAGE/{{ s3_prefix_filter }}/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FORCE = TRUE
ON_ERROR = CONTINUE;