from __future__ import annotations

from typing import Dict

from pyspark.sql import SparkSession


DEFAULT_JAR_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def build_spark_session(app_name: str, extra_configs: Dict[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            ",".join(DEFAULT_JAR_PACKAGES),
        )
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def configure_s3(spark: SparkSession, endpoint: str, access_key: str, secret_key: str) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    if endpoint.startswith("https://"):
        host = endpoint[len("https://") :]
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    elif endpoint.startswith("http://"):
        host = endpoint[len("http://") :]
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    else:
        host = endpoint
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    hadoop_conf.set("fs.s3a.endpoint", host)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
