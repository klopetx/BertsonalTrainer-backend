from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import Row, types as T

from bertsonal_spark.config import StreamingConfig
from bertsonal_spark.streaming.main import (
    EVENT_SCHEMA,
    _dictionary_udf,
    _normalize_udf,
    _process_silver_batch,
    _rhyme_udf,
    _sessions_df,
    _words_df,
)


PARSED_SCHEMA = T.StructType(
    [
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
        T.StructField("payload_hash", T.StringType(), False),
        T.StructField("data", EVENT_SCHEMA, False),
        T.StructField("processed_at", T.TimestampType(), False),
        T.StructField("source_record_id", T.StringType(), False),
    ]
)


def _parsed_row(
    *,
    event_id: str,
    session_id: str,
    schema_version: str = "1.0",
    business_date: str = "2024-05-01",
    submitted_words: list[str] | None = None,
    completed_at: str = "2024-05-01T22:15:00Z",
    timestamp: datetime,
) -> tuple:
    submitted_words = submitted_words or []
    data = Row(
        schema_version=schema_version,
        event_id=event_id,
        session_id=session_id,
        user_id="user-1",
        business_date=business_date,
        rhyme_id="rhyme-1",
        rhyme="ari",
        started_at="2024-05-01T22:00:00Z",
        completed_at=completed_at,
        submitted_words=submitted_words,
    )
    return (
        "session-events",
        0,
        0,
        timestamp,
        f"hash-{event_id}",
        data,
        datetime(2024, 5, 1, 22, 30, 0),
        f"session-events:0:{event_id}",
    )


def test_sessions_df_flags_contract_and_late_events(spark_session):
    normalize_udf = _normalize_udf()
    config = StreamingConfig(business_timezone="Europe/Madrid", business_cutoff_hour=1)

    parsed_df = spark_session.createDataFrame(
        [
            _parsed_row(
                event_id="11111111-1111-1111-1111-111111111111",
                session_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                submitted_words=["lari"],
                timestamp=datetime(2024, 5, 1, 22, 0, 0),
            ),
            _parsed_row(
                event_id="22222222-2222-2222-2222-222222222222",
                session_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                schema_version="2.0",
                submitted_words=["lari"],
                timestamp=datetime(2024, 5, 1, 22, 0, 0),
            ),
            _parsed_row(
                event_id="33333333-3333-3333-3333-333333333333",
                session_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                submitted_words=["lari"],
                timestamp=datetime(2024, 5, 3, 0, 30, 0),
            ),
        ],
        schema=PARSED_SCHEMA,
    )

    sessions_df = _sessions_df(parsed_df, normalize_udf, config)
    statuses = {
        row.event_id: (row.contract_valid, row.is_late_event)
        for row in sessions_df.select("event_id", "contract_valid", "is_late_event").collect()
    }

    assert statuses["11111111-1111-1111-1111-111111111111"] == (True, False)
    assert statuses["22222222-2222-2222-2222-222222222222"][0] is False
    assert statuses["33333333-3333-3333-3333-333333333333"] == (True, True)


def test_words_df_applies_validation_rules(spark_session):
    normalize_udf = _normalize_udf()
    config = StreamingConfig(business_timezone="Europe/Madrid", business_cutoff_hour=1)
    parsed_df = spark_session.createDataFrame(
        [
            _parsed_row(
                event_id="44444444-4444-4444-4444-444444444444",
                session_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                submitted_words=["Lari", "lari", "note", "moro"],
                timestamp=datetime(2024, 5, 1, 22, 0, 0),
            )
        ],
        schema=PARSED_SCHEMA,
    )

    sessions_df = _sessions_df(parsed_df, normalize_udf, config)
    valid_sessions_df = sessions_df.filter("contract_valid AND NOT is_late_event")

    dictionary_bc = spark_session.sparkContext.broadcast({"lari", "moro"})
    dictionary_udf = _dictionary_udf(dictionary_bc)
    rhyme_udf = _rhyme_udf()

    words_df = _words_df(valid_sessions_df, normalize_udf, dictionary_udf, rhyme_udf)
    results = [
        (row.word_index, row.original_word.strip(), row.is_valid, row.validation_reason)
        for row in words_df.orderBy("word_index").select(
            "word_index", "original_word", "is_valid", "validation_reason"
        ).collect()
    ]

    assert results[0] == (0, "Lari", True, None)
    assert results[1] == (1, "lari", False, "duplicate_in_session")
    assert results[2] == (2, "note", False, "not_in_dictionary")
    assert results[3] == (3, "moro", False, "rhyme_mismatch")


def test_process_silver_batch_writes_outputs(tmp_path, spark_session, monkeypatch):
    normalize_udf = _normalize_udf()
    config = StreamingConfig(
        silver_sessions_path=str(tmp_path / "silver_sessions"),
        silver_words_path=str(tmp_path / "silver_words"),
        checkpoint_root=str(tmp_path / "chk"),
        business_timezone="Europe/Madrid",
        business_cutoff_hour=1,
    )

    parsed_df = spark_session.createDataFrame(
        [
            _parsed_row(
                event_id="55555555-5555-5555-5555-555555555555",
                session_id="eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                business_date="2026-09-03",
                submitted_words=[" Lari ", "moro"],
                timestamp=datetime(2026, 9, 3, 21, 0, 0),
            )
        ],
        schema=PARSED_SCHEMA,
    )

    sessions_df = _sessions_df(parsed_df, normalize_udf, config)

    dictionary_bc = spark_session.sparkContext.broadcast({"lari"})
    dictionary_udf = _dictionary_udf(dictionary_bc)
    rhyme_udf = _rhyme_udf()

    captured_rows = {}

    def fake_write(batch_df, batch_id, cfg):
        captured_rows["count"] = batch_df.count()
        captured_rows["valid_sum"] = batch_df.agg({"valid_word_count": "max"}).collect()[0][0]

    monkeypatch.setattr("bertsonal_spark.streaming.main._write_provisional_scores", fake_write)

    _process_silver_batch(
        sessions_df,
        batch_id=0,
        config=config,
        normalize_udf=normalize_udf,
        dictionary_udf=dictionary_udf,
        rhyme_udf=rhyme_udf,
    )

    silver_sessions_dir = Path(config.silver_sessions_path)
    silver_words_dir = Path(config.silver_words_path)
    assert silver_sessions_dir.exists()
    assert silver_words_dir.exists()

    sessions_df_out = spark_session.read.parquet(str(silver_sessions_dir))
    words_df_out = spark_session.read.parquet(str(silver_words_dir))

    assert sessions_df_out.count() == 1
    assert words_df_out.count() == 2

    assert captured_rows.get("count") == 1
    assert captured_rows.get("valid_sum") == 1
