from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, time, timedelta
from typing import List, Sequence
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from bertsonal_spark.common.spark_utils import build_spark_session, configure_s3
from bertsonal_spark.config import BatchConfig


logger = logging.getLogger(__name__)


class CutoffNotReachedError(RuntimeError):
    """Raised when the batch is invoked before the agreed cutoff time."""


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the daily Spark batch job.")
    parser.add_argument("--business-date", required=True, help="Business date to process (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Bypass cutoff guardrails (local/testing only)")
    return parser.parse_args(argv)


def parse_business_date(raw_value: str) -> date:
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(f"Invalid date '{raw_value}'. Expected YYYY-MM-DD") from exc


def _ensure_cutoff_passed(target_date: date, config: BatchConfig, *, force: bool, now: datetime | None = None) -> None:
    if force:
        return

    tz = ZoneInfo(config.business_timezone)
    now_local = now.astimezone(tz) if now else datetime.now(tz)
    cutoff_dt = datetime.combine(target_date + timedelta(days=1), time(hour=config.business_cutoff_hour), tz)
    if now_local < cutoff_dt:
        raise CutoffNotReachedError(
            f"Cutoff not reached for {target_date}; cutoff is {cutoff_dt.isoformat()}"
        )


def _load_silver_sessions(spark: SparkSession, config: BatchConfig, target_date: date) -> DataFrame | None:
    partition_path = f"{config.silver_sessions_path}/business_date={target_date.isoformat()}"
    try:
        df = spark.read.parquet(partition_path)
    except AnalysisException:
        logger.info("No Silver sessions found for %s (path %s)", target_date, partition_path)
        return None
    return df.withColumn(
        "business_date", F.to_date(F.lit(target_date.isoformat()), format="yyyy-MM-dd")
    )


def _deduplicate_sessions(sessions_df: DataFrame, target_date: date) -> DataFrame:
    enriched = sessions_df.withColumn(
        "business_date", F.to_date(F.lit(target_date.isoformat()), format="yyyy-MM-dd")
    )
    event_window = Window.partitionBy("event_id").orderBy(F.col("kafka_timestamp"))
    user_day_window = Window.partitionBy("user_id", "business_date").orderBy(F.col("kafka_timestamp"))

    first_events = enriched.withColumn("event_rank", F.row_number().over(event_window)).filter(
        F.col("event_rank") == 1
    )
    accepted = first_events.withColumn("user_rank", F.row_number().over(user_day_window)).filter(
        F.col("user_rank") == 1
    )
    return accepted.drop("event_rank", "user_rank")


def _build_daily_scores_df(accepted_df: DataFrame) -> DataFrame:
    base = accepted_df.select(
        "business_date",
        "user_id",
        "session_id",
        "rhyme_id",
        "valid_word_count",
    )

    base = base.withColumn("originality_score", F.lit(None).cast("double"))
    base = base.withColumn("rhyme_difficulty", F.lit(None).cast("double"))
    base = base.withColumn("final_score", F.col("valid_word_count").cast("double"))

    ranking_window = Window.partitionBy("business_date").orderBy(
        F.col("final_score").desc(), F.col("user_id")
    )
    base = base.withColumn("rank_position", F.row_number().over(ranking_window))
    base = base.withColumn("calculated_at", F.current_timestamp())
    return base


def _build_rhyme_metrics_df(accepted_df: DataFrame) -> DataFrame:
    metrics = (
        accepted_df.groupBy("business_date")
        .agg(
            F.first("rhyme_id").alias("rhyme_id"),
            F.first("rhyme").alias("rhyme"),
            F.count("*").alias("participants"),
            F.sum(F.col("valid_word_count")).alias("total_valid_words"),
            (
                F.sum(F.col("valid_word_count"))
                / F.when(F.count("*") == 0, F.lit(1)).otherwise(F.count("*"))
            ).alias("average_valid_words_per_session"),
        )
        .withColumn("rhyme_difficulty", F.lit(None).cast("double"))
        .withColumn("calculated_at", F.current_timestamp())
    )
    return metrics


def _collect_rows(df: DataFrame, columns: Sequence[str]) -> List[tuple]:
    if df.rdd.isEmpty():
        return []
    return [tuple(row[col] for col in columns) for row in df.select(*columns).collect()]


def _ensure_gold_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.daily_scores (
                business_date DATE NOT NULL,
                user_id TEXT NOT NULL,
                session_id UUID NOT NULL,
                rhyme_id TEXT NOT NULL,
                valid_word_count INTEGER NOT NULL CHECK (valid_word_count >= 0),
                originality_score NUMERIC,
                rhyme_difficulty NUMERIC,
                final_score NUMERIC,
                rank_position INTEGER,
                calculated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (business_date, user_id),
                UNIQUE (session_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.rhyme_daily_metrics (
                business_date DATE PRIMARY KEY,
                rhyme_id TEXT NOT NULL,
                rhyme TEXT NOT NULL,
                participants INTEGER NOT NULL,
                total_valid_words INTEGER NOT NULL,
                average_valid_words_per_session NUMERIC NOT NULL,
                rhyme_difficulty NUMERIC,
                calculated_at TIMESTAMPTZ NOT NULL
            );
            """
        )


def _write_gold_tables(
    business_date: date,
    daily_rows: List[tuple],
    metrics_rows: List[tuple],
    config: BatchConfig,
) -> None:
    conn = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        user=config.postgres_user,
        password=config.postgres_password,
        dbname=config.postgres_db,
    )
    try:
        _ensure_gold_tables(conn)
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM gold.daily_scores WHERE business_date = %s", (business_date,))
                cur.execute("DELETE FROM gold.rhyme_daily_metrics WHERE business_date = %s", (business_date,))

                if daily_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO gold.daily_scores (
                            business_date,
                            user_id,
                            session_id,
                            rhyme_id,
                            valid_word_count,
                            originality_score,
                            rhyme_difficulty,
                            final_score,
                            rank_position,
                            calculated_at
                        ) VALUES %s
                        """,
                        daily_rows,
                    )

                if metrics_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO gold.rhyme_daily_metrics (
                            business_date,
                            rhyme_id,
                            rhyme,
                            participants,
                            total_valid_words,
                            average_valid_words_per_session,
                            rhyme_difficulty,
                            calculated_at
                        ) VALUES %s
                        """,
                        metrics_rows,
                    )
    finally:
        conn.close()


def run_batch_for_date(target_date: date, *, force: bool = False, config: BatchConfig | None = None) -> bool:
    config = config or BatchConfig()
    _ensure_cutoff_passed(target_date, config, force=force)

    spark = build_spark_session(config.app_name)
    configure_s3(spark, config.minio_endpoint, config.minio_access_key, config.minio_secret_key)

    try:
        sessions_df = _load_silver_sessions(spark, config, target_date)
        if sessions_df is None:
            return False

        deduped_df = _deduplicate_sessions(sessions_df, target_date).cache()
        if deduped_df.rdd.isEmpty():
            logger.info("No accepted sessions after deduplication for %s", target_date)
            deduped_df.unpersist()
            return False

        daily_scores_df = _build_daily_scores_df(deduped_df).cache()
        rhyme_metrics_df = _build_rhyme_metrics_df(deduped_df).cache()
        deduped_df.unpersist()

        daily_rows = _collect_rows(
            daily_scores_df,
            [
                "business_date",
                "user_id",
                "session_id",
                "rhyme_id",
                "valid_word_count",
                "originality_score",
                "rhyme_difficulty",
                "final_score",
                "rank_position",
                "calculated_at",
            ],
        )
        metrics_rows = _collect_rows(
            rhyme_metrics_df,
            [
                "business_date",
                "rhyme_id",
                "rhyme",
                "participants",
                "total_valid_words",
                "average_valid_words_per_session",
                "rhyme_difficulty",
                "calculated_at",
            ],
        )
        daily_scores_df.unpersist()
        rhyme_metrics_df.unpersist()

        if not daily_rows:
            logger.info("No qualifying sessions for %s", target_date)
            return False

        _write_gold_tables(target_date, daily_rows, metrics_rows, config)
        logger.info(
            "Batch completed for %s (daily rows=%s, rhyme metrics=%s)",
            target_date,
            len(daily_rows),
            len(metrics_rows),
        )
        return True
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    _setup_logging()
    args = parse_args(argv)
    business_date = parse_business_date(args.business_date)
    try:
        changed = run_batch_for_date(business_date, force=args.force)
    except CutoffNotReachedError as exc:
        logger.error(str(exc))
        return 2
    except Exception:  # pragma: no cover
        logger.exception("Batch failed for %s", business_date)
        return 1

    return 0 if changed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
