"""
Config 패키지입니다.
data_filtering_config, s3_config, snowflake_config 모듈의 변수들을 사용할 수 있도록 export합니다.
"""

from custom_modules.config.data_filtering_config import *
from custom_modules.config.s3_config import *
from custom_modules.config.snowflake_config import *

__all__ = [
    # data_filtering_config
    "DEFAULT_WEEKLY_START_DATE",
    "DEFAULT_WEEKLY_END_DATE",
    "DEFAULT_FREQ",
    # s3_config
    "S3_BUCKET_NAME",
    "RAW_FOLDER",
    "PROCESSED_FOLDER",
    # snowflake_config
    "SQL_DIR",
]