from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def get_spark_session(aws_access_key, aws_secret_key, master=None, app_name="Spark Application", extra_conf=None):
    """
    SparkSession 생성 함수.
    aws_access_key와 aws_secret_key를 사용하여 S3 접근 설정을 추가합니다.
    master가 지정되지 않으면 기본값으로 동작합니다.
    extra_conf: 추가 설정 딕셔너리
    """
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    # S3 접속 기본 설정
    builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # 동적 할당 및 외부 셔플 서비스 활성화
    # builder = builder.config("spark.dynamicAllocation.enabled", "true")
    # builder = builder.config("spark.shuffle.service.enabled", "true")

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    else:
        # 기본 예시 설정
        builder = builder.config("spark.executor.memory", "6g")
        builder = builder.config("spark.executor.cores", "2")
        builder = builder.config("spark.executor.instances", "2")
        builder = builder.config("spark.driver.memory", "1g")
        # builder = builder.config("spark.sql.shuffle.partitions", "200")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    logger.info(f"Spark 세션 생성 완료. TimeZone: {spark.conf.get('spark.sql.session.timeZone')}")
    return spark