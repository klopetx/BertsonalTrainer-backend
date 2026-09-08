from __future__ import annotations

import logging
import signal
import sys
from pathlib import Path
from typing import List, Tuple

import boto3
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

from bertsonal_spark.common.dictionary import load_dictionary
from bertsonal_spark.common.spark_utils import build_spark_session, configure_s3
from bertsonal_spark.common.validation import (
    is_dictionary_word,
    matches_rhyme,
    normalize_token,
)
from bertsonal_spark.config import StreamingConfig


SCHEMA_INITIALIZED = False
VALID_SCHEMA_VERSION = "1.0"
UUID_REGEX = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


logger = logging.getLogger(__name__)


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("session_id", T.StringType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("business_date", T.StringType(), True),
        T.StructField("rhyme_id", T.StringType(), True),
        T.StructField("rhyme", T.StringType(), True),
        T.StructField("started_at", T.StringType(), True),
        T.StructField("completed_at", T.StringType(), True),
        T.StructField("submitted_words", T.ArrayType(T.StringType()), True),
    ]
)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )


def _bucket_from_path(path: str) -> str | None:
    if not path.startswith("s3a://"):
        return None
    remainder = path[len("s3a://") :]
    bucket, *_ = remainder.split("/", 1)
    return bucket or None


def _ensure_buckets(config: StreamingConfig) -> None:
    session = boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key,
        region_name="us-east-1",
    )

    paths = [
        config.bronze_base_path,
        config.silver_sessions_path,
        config.silver_words_path,
        config.checkpoint_root,
    ]
    buckets = {bucket for bucket in (_bucket_from_path(p) for p in paths) if bucket}

    for bucket in buckets:
        try:
            session.create_bucket(Bucket=bucket)
        except session.exceptions.BucketAlreadyOwnedByYou:
            continue
        except session.exceptions.BucketAlreadyExists:
            continue


def _broadcast_dictionary(spark: SparkSession, dictionary_path: Path):
    words = load_dictionary(dictionary_path)
    return spark.sparkContext.broadcast(words)


def _normalize_udf():
    return F.udf(normalize_token, T.StringType())


def _dictionary_udf(dictionary_bc):
    def _contains(word: str | None) -> bool:
        return is_dictionary_word(word, dictionary_bc.value)

    return F.udf(_contains, T.BooleanType())


def _rhyme_udf():
    def _matches(word: str | None, rhyme: str | None) -> bool:
        return matches_rhyme(word, rhyme)

    return F.udf(_matches, T.BooleanType())


def _build_kafka_source(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def _bronze_df(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),
        F.col("value").alias("raw_payload"),
    )


def _parsed_df(kafka_df: DataFrame) -> DataFrame:
    payload_str = F.col("value").cast("string")
    return (
        kafka_df.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
            F.sha2(payload_str, 256).alias("payload_hash"),
            F.from_json(payload_str, EVENT_SCHEMA).alias("data"),
            F.current_timestamp().alias("processed_at"),
        )
        .filter(F.col("data").isNotNull())
        .withColumn(
            "source_record_id",
            F.concat_ws(":", F.col("topic"), F.col("partition"), F.col("offset")),
        )
    )


def _sessions_df(parsed_df: DataFrame, normalize_udf, config: StreamingConfig) -> DataFrame:
    cutoff_hour = max(0, min(23, int(config.business_cutoff_hour)))
    sessions = parsed_df.select(
        "source_record_id",
        "payload_hash",
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        "processed_at",
        F.col("data.schema_version").alias("schema_version"),
        F.col("data.event_id").alias("event_id"),
        F.col("data.session_id").alias("session_id"),
        F.col("data.user_id").alias("user_id"),
        F.to_date(F.col("data.business_date"), "yyyy-MM-dd").alias("business_date"),
        F.col("data.rhyme_id").alias("rhyme_id"),
        F.col("data.rhyme").alias("rhyme"),
        F.to_timestamp(F.col("data.started_at")).alias("started_at"),
        F.to_timestamp(F.col("data.completed_at")).alias("completed_at"),
        F.col("data.submitted_words").alias("submitted_words"),
    )

    sessions = sessions.withColumn("normalized_rhyme", normalize_udf(F.col("rhyme")))

    sessions = sessions.withColumn(
        "submitted_word_count",
        F.when(F.col("submitted_words").isNull(), F.lit(0)).otherwise(
            F.size(F.col("submitted_words"))
        ),
    )

    def _non_empty_string(column_name: str):
        column = F.col(column_name).cast("string")
        return column.isNotNull() & (F.length(F.trim(column)) > 0)

    sessions = sessions.withColumn(
        "valid_schema_version", F.col("schema_version") == F.lit(VALID_SCHEMA_VERSION)
    ).withColumn(
        "valid_event_id",
        _non_empty_string("event_id") & F.col("event_id").rlike(UUID_REGEX),
    ).withColumn(
        "valid_session_id",
        _non_empty_string("session_id") & F.col("session_id").rlike(UUID_REGEX),
    ).withColumn(
        "valid_user_id", _non_empty_string("user_id"),
    ).withColumn(
        "valid_business_date", F.col("business_date").isNotNull(),
    ).withColumn(
        "valid_rhyme_id", _non_empty_string("rhyme_id"),
    ).withColumn(
        "valid_rhyme", _non_empty_string("rhyme"),
    ).withColumn(
        "valid_timestamps",
        F.col("started_at").isNotNull()
        & F.col("completed_at").isNotNull()
        & (F.col("completed_at") >= F.col("started_at")),
    ).withColumn(
        "valid_submitted_words", F.col("submitted_words").isNotNull(),
    )

    sessions = sessions.withColumn(
        "contract_valid",
        F.col("valid_schema_version")
        & F.col("valid_event_id")
        & F.col("valid_session_id")
        & F.col("valid_user_id")
        & F.col("valid_business_date")
        & F.col("valid_rhyme_id")
        & F.col("valid_rhyme")
        & F.col("valid_timestamps")
        & F.col("valid_submitted_words"),
    )

    cutoff_local = F.to_timestamp(
        F.concat_ws(
            " ",
            F.date_format(F.date_add(F.col("business_date"), 1), "yyyy-MM-dd"),
            F.lit(f"{cutoff_hour:02d}:00:00"),
        ),
        "yyyy-MM-dd HH:mm:ss",
    )
    cutoff_utc = F.to_utc_timestamp(cutoff_local, config.business_timezone)
    sessions = sessions.withColumn("is_late_event", F.col("kafka_timestamp") >= cutoff_utc)

    return sessions


def _words_df(sessions_df: DataFrame, normalize_udf, dictionary_udf, rhyme_udf) -> DataFrame:
    exploded = sessions_df.select(
        "source_record_id",
        "event_id",
        "session_id",
        "user_id",
        "business_date",
        "rhyme_id",
        "normalized_rhyme",
        F.posexplode_outer(F.col("submitted_words")).alias("word_index", "original_word"),
    )

    words = exploded.withColumn(
        "normalized_word", normalize_udf(F.col("original_word"))
    )

    words = words.withColumn(
        "dictionary_valid", dictionary_udf(F.col("normalized_word"))
    ).withColumn(
        "rhyme_valid", rhyme_udf(F.col("normalized_word"), F.col("normalized_rhyme"))
    )

    dup_window = Window.partitionBy("session_id", "normalized_word").orderBy("word_index")
    words = words.withColumn(
        "duplicate_rank",
        F.when(F.col("normalized_word").isNull(), None).otherwise(F.row_number().over(dup_window)),
    )
    words = words.withColumn("duplicate_in_session", F.col("duplicate_rank") > 1)

    words = words.withColumn(
        "validation_reason",
        F.when(F.col("normalized_word").isNull(), F.lit("empty_or_invalid_token"))
        .when(~F.col("dictionary_valid"), F.lit("not_in_dictionary"))
        .when(~F.col("rhyme_valid"), F.lit("rhyme_mismatch"))
        .when(F.col("duplicate_in_session"), F.lit("duplicate_in_session"))
        .otherwise(F.lit(None)),
    )

    words = words.withColumn("is_valid", F.col("validation_reason").isNull())
    return words


def _session_metrics(words_df: DataFrame) -> DataFrame:
    return (
        words_df.groupBy("source_record_id")
        .agg(
            F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("valid_word_count"),
            F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("invalid_word_count"),
        )
        .fillna({"valid_word_count": 0, "invalid_word_count": 0})
    )


def _silver_sessions_output(sessions_df: DataFrame, metrics_df: DataFrame) -> DataFrame:
    enriched = sessions_df.join(metrics_df, "source_record_id", "left")
    enriched = enriched.fillna({"valid_word_count": 0, "invalid_word_count": 0})
    drop_cols = [
        "submitted_words",
        "normalized_rhyme",
        "valid_schema_version",
        "valid_event_id",
        "valid_session_id",
        "valid_user_id",
        "valid_business_date",
        "valid_rhyme_id",
        "valid_rhyme",
        "valid_timestamps",
        "valid_submitted_words",
        "contract_valid",
        "is_late_event",
    ]
    existing_drop_cols = [col for col in drop_cols if col in enriched.columns]
    if existing_drop_cols:
        enriched = enriched.drop(*existing_drop_cols)
    enriched = (
        enriched.withColumnRenamed("topic", "kafka_topic")
        .withColumnRenamed("partition", "kafka_partition")
        .withColumnRenamed("offset", "kafka_offset")
    )
    return enriched


def _words_output(words_df: DataFrame) -> DataFrame:
    return words_df.select(
        "source_record_id",
        "event_id",
        "session_id",
        "user_id",
        "business_date",
        "rhyme_id",
        "word_index",
        "original_word",
        "normalized_word",
        "is_valid",
        "validation_reason",
    )


def _write_partitioned_parquet(df: DataFrame, path: str, partition_cols: List[str]) -> None:
    if df.rdd.isEmpty():
        return
    writer = df.write.mode("append").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def _process_silver_batch(
    batch_df: DataFrame,
    batch_id: int,
    config: StreamingConfig,
    normalize_udf,
    dictionary_udf,
    rhyme_udf,
) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Skipping empty batch %s for silver outputs", batch_id)
        return

    processable_condition = F.col("contract_valid") & (~F.col("is_late_event"))
    valid_df = batch_df.filter(processable_condition).cache()
    dropped_df = batch_df.filter(~processable_condition)

    if not dropped_df.rdd.isEmpty():
        reason_counts = (
            dropped_df.withColumn(
                "drop_reason",
                F.when(~F.col("contract_valid"), F.lit("CONTRACT_VALIDATION_ERROR"))
                .when(F.col("is_late_event"), F.lit("LATE_EVENT"))
                .otherwise(F.lit("UNKNOWN")),
            )
            .groupBy("drop_reason")
            .count()
            .collect()
        )
        for row in reason_counts:
            logger.warning(
                "Batch %s dropped %s records due to %s",
                batch_id,
                row["count"],
                row["drop_reason"],
            )

    if valid_df.rdd.isEmpty():
        logger.info("No processable records in batch %s", batch_id)
        valid_df.unpersist()
        return

    words_df = _words_df(valid_df, normalize_udf, dictionary_udf, rhyme_udf).cache()
    metrics_df = _session_metrics(words_df)
    silver_sessions_df = _silver_sessions_output(valid_df, metrics_df).cache()
    words_output_df = _words_output(words_df)

    _write_partitioned_parquet(
        silver_sessions_df, config.silver_sessions_path, ["business_date"]
    )
    _write_partitioned_parquet(words_output_df, config.silver_words_path, ["business_date"])
    _write_provisional_scores(silver_sessions_df, batch_id, config)

    words_df.unpersist()
    silver_sessions_df.unpersist()
    valid_df.unpersist()


def _write_provisional_scores(batch_df: DataFrame, batch_id: int, config: StreamingConfig) -> None:
    rows = (
        batch_df.select(
            "event_id",
            "session_id",
            "user_id",
            "business_date",
            "rhyme_id",
            "valid_word_count",
            "kafka_timestamp",
            "processed_at",
        )
        .where(F.col("event_id").isNotNull())
        .collect()
    )

    if not rows:
        return

    payload: List[Tuple] = []
    for row in rows:
        payload.append(
            (
                row.event_id,
                row.session_id,
                row.user_id,
                row.business_date,
                row.rhyme_id,
                int(row.valid_word_count or 0),
                int(row.valid_word_count or 0),
                row.kafka_timestamp,
                row.processed_at,
            )
        )

    conn = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        user=config.postgres_user,
        password=config.postgres_password,
        dbname=config.postgres_db,
    )

    try:
        global SCHEMA_INITIALIZED
        if not SCHEMA_INITIALIZED:
            _ensure_provisional_table(conn)
            SCHEMA_INITIALIZED = True
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO serving.provisional_scores (
                        event_id,
                        session_id,
                        user_id,
                        business_date,
                        rhyme_id,
                        valid_word_count,
                        provisional_score,
                        kafka_timestamp,
                        calculated_at
                    ) VALUES %s
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    payload,
                )
    finally:
        conn.close()


def _ensure_provisional_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS serving;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS serving.provisional_scores (
                event_id UUID PRIMARY KEY,
                session_id UUID UNIQUE,
                user_id TEXT NOT NULL,
                business_date DATE NOT NULL,
                rhyme_id TEXT NOT NULL,
                valid_word_count INTEGER NOT NULL CHECK (valid_word_count >= 0),
                provisional_score INTEGER NOT NULL CHECK (provisional_score >= 0),
                kafka_timestamp TIMESTAMPTZ,
                calculated_at TIMESTAMPTZ NOT NULL,
                UNIQUE (user_id, business_date)
            );
            """
        )

        # Forward-only schema evolution: keep latency computation easy in SQL without backfills.
        cur.execute(
            "ALTER TABLE serving.provisional_scores ADD COLUMN IF NOT EXISTS kafka_timestamp TIMESTAMPTZ;"
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_provisional_scores_business_date
                ON serving.provisional_scores (business_date);
            """
        )


def main() -> int:
    _setup_logging()
    config = StreamingConfig()
    spark = build_spark_session(
        config.app_name,
        {"spark.sql.streaming.stateStore.maintenanceInterval": "300s"},
    )
    configure_s3(spark, config.minio_endpoint, config.minio_access_key, config.minio_secret_key)
    _ensure_buckets(config)

    dictionary_bc = _broadcast_dictionary(spark, Path(config.dictionary_path))
    normalize_udf = _normalize_udf()
    dictionary_udf = _dictionary_udf(dictionary_bc)
    rhyme_udf = _rhyme_udf()

    kafka_df = _build_kafka_source(spark, config)
    bronze_df = _bronze_df(kafka_df)
    parsed_df = _parsed_df(kafka_df)
    sessions_df = _sessions_df(parsed_df, normalize_udf, config)

    queries: List[StreamingQuery] = []

    queries.append(
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.bronze_base_path)
        .option("checkpointLocation", f"{config.checkpoint_root}/bronze")
        .partitionBy("ingestion_date")
        .start()
    )

    queries.append(
        sessions_df.writeStream.outputMode("update")
        .option("checkpointLocation", f"{config.checkpoint_root}/silver_pipeline")
        .foreachBatch(
            lambda df, batch_id: _process_silver_batch(
                df,
                batch_id,
                config,
                normalize_udf,
                dictionary_udf,
                rhyme_udf,
            )
        )
        .start()
    )

    def _shutdown(signum, frame):  # pragma: no cover
        logger.info("Received signal %s; stopping streaming queries", signum)
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
