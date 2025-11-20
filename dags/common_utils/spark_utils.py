import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(aws_access_key, aws_secret_key):
    """
    AWS Connection ID를 사용하여 AWS 자격 증명을 가져온 후 SparkSession을 생성
    """

    spark = SparkSession.builder \
        .appName("Extract & Transform Data") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # SparkSession Time Zone을 UTC로 설정
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    logger.info(f"Current TimeZone: {spark.conf.get("spark.sql.session.timeZone")}")

    return spark

