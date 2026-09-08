from __future__ import annotations

import argparse
import logging
import sys
import csv
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

from bertsonal_spark.common.spark_utils import build_spark_session, configure_s3
from bertsonal_spark.config import BatchConfig


logger = logging.getLogger(__name__)

MAX_DICTIONARY_SIZE = 74.0


class CutoffNotReachedError(RuntimeError):
    """Raised when the batch is invoked before the agreed cutoff time."""


def _week_start_date(value: date) -> date:
    # ISO-style week start: Monday
    return value - timedelta(days=value.weekday())


def _month_start_date(value: date) -> date:
    return value.replace(day=1)


def _month_end_date(value: date) -> date:
    month_start = _month_start_date(value)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month - timedelta(days=1)


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")


def _load_dictionary_counts(path: str) -> Dict[str, int]:
    dictionary_path = Path(path)
    if not dictionary_path.exists():
        raise FileNotFoundError(f"Dictionary file not found at {dictionary_path}")

    counts: Dict[str, int] = {}
    with dictionary_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or {"Ending", "Word"}.difference(reader.fieldnames):
            raise ValueError("Dictionary CSV must contain 'Ending' and 'Word' columns")
        for row in reader:
            ending = (row.get("Ending") or "").strip().lower()
            if not ending:
                continue
            counts[ending] = counts.get(ending, 0) + 1
    if not counts:
        raise ValueError(f"Dictionary at {dictionary_path} contains no usable entries")
    return counts


def _dictionary_counts_df(spark: SparkSession, counts: Dict[str, int]) -> DataFrame:
    rows = [(ending, count) for ending, count in counts.items()]
    if not rows:
        schema = T.StructType(
            [
                T.StructField("normalized_rhyme", T.StringType(), False),
                T.StructField("dictionary_word_count", T.IntegerType(), False),
            ]
        )
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows, ["normalized_rhyme", "dictionary_word_count"])


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


def _load_silver_words(spark: SparkSession, config: BatchConfig, target_date: date) -> DataFrame | None:
    partition_path = f"{config.silver_words_path}/business_date={target_date.isoformat()}"
    try:
        df = spark.read.parquet(partition_path)
    except AnalysisException:
        logger.info("No Silver session-words found for %s (path %s)", target_date, partition_path)
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


def _build_daily_scores_df(accepted_df: DataFrame, session_scores_df: DataFrame | None) -> DataFrame:
    base = accepted_df.select(
        "business_date",
        "user_id",
        "session_id",
        "rhyme_id",
        "valid_word_count",
    )

    if session_scores_df is not None:
        base = base.join(
            session_scores_df.select(
                "session_id", "daily_score", "hardness_weighted_daily_score"
            ),
            "session_id",
            "left",
        )
    else:
        base = base.withColumn("daily_score", F.lit(None).cast("double"))
        base = base.withColumn("hardness_weighted_daily_score", F.lit(None).cast("double"))

    base = base.withColumn(
        "daily_score",
        F.when(F.col("daily_score").isNull(), F.lit(0.0)).otherwise(F.col("daily_score")),
    )
    base = base.withColumn(
        "hardness_weighted_daily_score",
        F.when(F.col("hardness_weighted_daily_score").isNull(), F.col("daily_score"))
        .otherwise(F.col("hardness_weighted_daily_score")),
    )

    base = base.withColumn("originality_score", F.col("daily_score"))
    base = base.withColumn("final_score", F.col("hardness_weighted_daily_score"))

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
        .withColumn("calculated_at", F.current_timestamp())
    )
    return metrics


def _compute_word_and_session_scores(
    accepted_sessions_df: DataFrame,
    words_df: DataFrame,
    dictionary_counts_df: DataFrame,
) -> Tuple[DataFrame | None, DataFrame | None]:
    if words_df is None or accepted_sessions_df is None:
        return None, None

    session_columns = ["session_id", "user_id", "business_date", "rhyme_id", "rhyme"]
    session_lookup = accepted_sessions_df.select(*session_columns).dropDuplicates(["session_id"])

    words_for_join = words_df.drop("business_date", "user_id", "rhyme_id")

    joined_words = (
        words_for_join.join(session_lookup, "session_id", "inner")
        .filter(F.col("is_valid") & F.col("normalized_word").isNotNull())
        .dropDuplicates(["session_id", "normalized_word"])
        .cache()
    )

    if joined_words.rdd.isEmpty():
        joined_words.unpersist()
        return None, None

    word_usage = (
        joined_words.groupBy("business_date", "normalized_word")
        .agg(F.countDistinct("user_id").alias("user_count"))
        .withColumn(
            "daily_repetitions",
            F.when(F.col("user_count") <= 1, F.lit(0)).otherwise(F.col("user_count") - 1),
        )
    )

    rmax_df = (
        word_usage.groupBy("business_date")
        .agg(F.max("daily_repetitions").alias("rmax"))
        .withColumn("rmax", F.when(F.col("rmax").isNull(), F.lit(0)).otherwise(F.col("rmax")))
    )

    word_usage = word_usage.join(rmax_df, "business_date", "left")
    word_usage = word_usage.withColumn(
        "daily_word_score",
        F.when(F.col("rmax") <= 0, F.lit(1.0)).otherwise(
            F.lit(1.0) - (F.col("daily_repetitions") / F.col("rmax")) * F.lit(0.5)
        ),
    )

    word_metrics = joined_words.join(
        word_usage.select(
            "business_date", "normalized_word", "daily_repetitions", "daily_word_score"
        ),
        ["business_date", "normalized_word"],
        "left",
    )

    session_scores = (
        word_metrics.groupBy("session_id")
        .agg(F.sum("daily_word_score").alias("daily_score"))
        .join(session_lookup, "session_id", "left")
        .withColumn("normalized_rhyme", F.lower(F.col("rhyme")))
        .join(dictionary_counts_df, "normalized_rhyme", "left")
        .withColumn(
            "dictionary_word_count",
            F.coalesce(F.col("dictionary_word_count"), F.lit(0)),
        )
    )

    hardness_multiplier = F.greatest(
        F.lit(0.0),
        F.lit(1.0)
        - (F.col("dictionary_word_count") / F.lit(MAX_DICTIONARY_SIZE)) * F.lit(0.6),
    )
    session_scores = session_scores.withColumn(
        "hardness_weighted_daily_score", F.col("daily_score") * hardness_multiplier
    )

    session_scores = session_scores.drop("normalized_rhyme").cache()

    word_metrics = word_metrics.join(
        session_scores.select("session_id", "daily_score", "hardness_weighted_daily_score"),
        "session_id",
        "left",
    ).withColumn("calculated_at", F.current_timestamp())

    word_metrics = word_metrics.cache()

    joined_words.unpersist()

    return word_metrics, session_scores


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
                daily_score NUMERIC,
                hardness_weighted_daily_score NUMERIC,
                originality_score NUMERIC,
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
            CREATE TABLE IF NOT EXISTS gold.session_word_metrics (
                business_date DATE NOT NULL,
                session_id UUID NOT NULL,
                user_id TEXT NOT NULL,
                rhyme_id TEXT NOT NULL,
                normalized_word TEXT NOT NULL,
                daily_repetitions INTEGER NOT NULL,
                daily_word_score NUMERIC NOT NULL,
                daily_score NUMERIC NOT NULL,
                hardness_weighted_daily_score NUMERIC NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (business_date, session_id, normalized_word)
            );
            """
        )
        cur.execute(
            "ALTER TABLE gold.daily_scores ADD COLUMN IF NOT EXISTS daily_score NUMERIC;"
        )
        cur.execute(
            """
            ALTER TABLE gold.daily_scores
            ADD COLUMN IF NOT EXISTS hardness_weighted_daily_score NUMERIC;
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
                calculated_at TIMESTAMPTZ NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.weekly_rankings (
                week_start_date DATE NOT NULL,
                user_id TEXT NOT NULL,
                total_score NUMERIC NOT NULL,
                average_score NUMERIC NOT NULL,
                days_played INTEGER NOT NULL,
                rank_by_total INTEGER NOT NULL,
                rank_by_average INTEGER NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (week_start_date, user_id)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_weekly_rank_total
            ON gold.weekly_rankings (week_start_date, rank_by_total);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_weekly_rank_average
            ON gold.weekly_rankings (week_start_date, rank_by_average);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.monthly_rankings (
                month_start_date DATE NOT NULL,
                user_id TEXT NOT NULL,
                total_score NUMERIC NOT NULL,
                average_score NUMERIC NOT NULL,
                days_played INTEGER NOT NULL,
                rank_by_total INTEGER NOT NULL,
                rank_by_average INTEGER NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (month_start_date, user_id)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monthly_rank_total
            ON gold.monthly_rankings (month_start_date, rank_by_total);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monthly_rank_average
            ON gold.monthly_rankings (month_start_date, rank_by_average);
            """
        )

        # If these columns existed in earlier iterations, remove them to keep the Gold schema aligned
        # with the current scoring model (hardness is the sole difficulty factor).
        cur.execute("ALTER TABLE gold.daily_scores DROP COLUMN IF EXISTS rhyme_difficulty;")
        cur.execute("ALTER TABLE gold.rhyme_daily_metrics DROP COLUMN IF EXISTS rhyme_difficulty;")
    conn.commit()


def _write_gold_tables(
    business_date: date,
    daily_rows: List[tuple],
    metrics_rows: List[tuple],
    word_metric_rows: List[tuple],
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
                week_start = _week_start_date(business_date)
                week_end = week_start + timedelta(days=6)
                month_start = _month_start_date(business_date)
                month_end = _month_end_date(business_date)

                cur.execute("DELETE FROM gold.daily_scores WHERE business_date = %s", (business_date,))
                cur.execute("DELETE FROM gold.rhyme_daily_metrics WHERE business_date = %s", (business_date,))
                cur.execute(
                    "DELETE FROM gold.session_word_metrics WHERE business_date = %s",
                    (business_date,),
                )

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
                            daily_score,
                            hardness_weighted_daily_score,
                            originality_score,
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
                            calculated_at
                        ) VALUES %s
                        """,
                        metrics_rows,
                    )

                if word_metric_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO gold.session_word_metrics (
                            business_date,
                            session_id,
                            user_id,
                            rhyme_id,
                            normalized_word,
                            daily_repetitions,
                            daily_word_score,
                            daily_score,
                            hardness_weighted_daily_score,
                            calculated_at
                        ) VALUES %s
                        """,
                        word_metric_rows,
                    )

                # Rankings are recomputed for the affected week/month scopes using the (possibly updated)
                # gold.daily_scores rows now visible in this transaction.
                cur.execute(
                    "DELETE FROM gold.weekly_rankings WHERE week_start_date = %s",
                    (week_start,),
                )
                cur.execute(
                    "DELETE FROM gold.monthly_rankings WHERE month_start_date = %s",
                    (month_start,),
                )

                cur.execute(
                    """
                    WITH per_user AS (
                        SELECT
                            user_id,
                            SUM(final_score) AS total_score,
                            AVG(final_score) AS average_score,
                            COUNT(DISTINCT business_date) AS days_played
                        FROM gold.daily_scores
                        WHERE business_date BETWEEN %s AND %s
                        GROUP BY user_id
                    ), ranked AS (
                        SELECT
                            %s::date AS week_start_date,
                            user_id,
                            total_score,
                            average_score,
                            days_played,
                            ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id) AS rank_by_total,
                            ROW_NUMBER() OVER (ORDER BY average_score DESC, user_id) AS rank_by_average
                        FROM per_user
                    )
                    SELECT
                        week_start_date,
                        user_id,
                        total_score,
                        average_score,
                        days_played,
                        rank_by_total,
                        rank_by_average,
                        CURRENT_TIMESTAMP
                    FROM ranked
                    """,
                    (week_start, week_end, week_start),
                )
                weekly_rows = cur.fetchall()
                if weekly_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO gold.weekly_rankings (
                            week_start_date,
                            user_id,
                            total_score,
                            average_score,
                            days_played,
                            rank_by_total,
                            rank_by_average,
                            calculated_at
                        ) VALUES %s
                        """,
                        weekly_rows,
                    )

                cur.execute(
                    """
                    WITH per_user AS (
                        SELECT
                            user_id,
                            SUM(final_score) AS total_score,
                            AVG(final_score) AS average_score,
                            COUNT(DISTINCT business_date) AS days_played
                        FROM gold.daily_scores
                        WHERE business_date BETWEEN %s AND %s
                        GROUP BY user_id
                    ), ranked AS (
                        SELECT
                            %s::date AS month_start_date,
                            user_id,
                            total_score,
                            average_score,
                            days_played,
                            ROW_NUMBER() OVER (ORDER BY total_score DESC, user_id) AS rank_by_total,
                            ROW_NUMBER() OVER (ORDER BY average_score DESC, user_id) AS rank_by_average
                        FROM per_user
                    )
                    SELECT
                        month_start_date,
                        user_id,
                        total_score,
                        average_score,
                        days_played,
                        rank_by_total,
                        rank_by_average,
                        CURRENT_TIMESTAMP
                    FROM ranked
                    """,
                    (month_start, month_end, month_start),
                )
                monthly_rows = cur.fetchall()
                if monthly_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO gold.monthly_rankings (
                            month_start_date,
                            user_id,
                            total_score,
                            average_score,
                            days_played,
                            rank_by_total,
                            rank_by_average,
                            calculated_at
                        ) VALUES %s
                        """,
                        monthly_rows,
                    )
    finally:
        conn.close()


def run_batch_for_date(target_date: date, *, force: bool = False, config: BatchConfig | None = None) -> bool:
    config = config or BatchConfig()
    _ensure_cutoff_passed(target_date, config, force=force)

    spark = build_spark_session(config.app_name)
    configure_s3(spark, config.minio_endpoint, config.minio_access_key, config.minio_secret_key)
    dictionary_counts = _load_dictionary_counts(config.dictionary_path)
    dictionary_counts_df = _dictionary_counts_df(spark, dictionary_counts)

    try:
        sessions_df = _load_silver_sessions(spark, config, target_date)
        if sessions_df is None:
            return False

        deduped_df = _deduplicate_sessions(sessions_df, target_date).cache()
        if deduped_df.rdd.isEmpty():
            logger.info("No accepted sessions after deduplication for %s", target_date)
            deduped_df.unpersist()
            return False

        words_df = _load_silver_words(spark, config, target_date)
        word_metrics_df: DataFrame | None = None
        session_scores_df: DataFrame | None = None
        if words_df is not None:
            word_metrics_df, session_scores_df = _compute_word_and_session_scores(
                deduped_df, words_df, dictionary_counts_df
            )

        daily_scores_df = _build_daily_scores_df(deduped_df, session_scores_df).cache()
        rhyme_metrics_df = _build_rhyme_metrics_df(deduped_df).cache()
        deduped_df.unpersist()
        if session_scores_df is not None:
            session_scores_df.unpersist()

        daily_rows = _collect_rows(
            daily_scores_df,
            [
                "business_date",
                "user_id",
                "session_id",
                "rhyme_id",
                "valid_word_count",
                "daily_score",
                "hardness_weighted_daily_score",
                "originality_score",
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
                "calculated_at",
            ],
        )
        word_rows: List[tuple] = []
        if word_metrics_df is not None:
            word_rows = _collect_rows(
                word_metrics_df,
                [
                    "business_date",
                    "session_id",
                    "user_id",
                    "rhyme_id",
                    "normalized_word",
                    "daily_repetitions",
                    "daily_word_score",
                    "daily_score",
                    "hardness_weighted_daily_score",
                    "calculated_at",
                ],
            )
            word_metrics_df.unpersist()

        daily_scores_df.unpersist()
        rhyme_metrics_df.unpersist()

        if not daily_rows:
            logger.info("No qualifying sessions for %s", target_date)
            return False

        _write_gold_tables(target_date, daily_rows, metrics_rows, word_rows, config)
        logger.info(
            "Batch completed for %s (daily rows=%s, rhyme metrics=%s, word rows=%s)",
            target_date,
            len(daily_rows),
            len(metrics_rows),
            len(word_rows),
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
