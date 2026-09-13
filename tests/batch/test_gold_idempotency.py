from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest


def _connect_or_skip():
    try:
        import psycopg2  # type: ignore
    except Exception:  # pragma: no cover
        pytest.skip("psycopg2 not available in this environment")

    host = os.environ.get("POSTGRES_TEST_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_TEST_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "bertsonal")
    password = os.environ.get("POSTGRES_PASSWORD", "bertsonal_pw")
    dbname = os.environ.get("POSTGRES_DB", "bertsonal")

    try:
        return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    except Exception:  # pragma: no cover
        pytest.skip(f"Postgres not reachable on {host}:{port}; start compose infra to run this test")


def _fetchall(cur, query: str, params: tuple):
    cur.execute(query, params)
    return cur.fetchall()


def test_gold_write_is_idempotent_for_same_date():
    """Gold replace-by-scope must not create duplicates or drift on rerun.

    This is an integration-style test: it talks to a real Postgres.
    It auto-skips if Postgres isn't available.
    """

    from bertsonal_spark.batch.main import (
        _month_start_date,
        _week_start_date,
        _write_gold_tables,
    )
    from bertsonal_spark.config import BatchConfig

    cfg = BatchConfig(
        postgres_host=os.environ.get("POSTGRES_TEST_HOST", "localhost"),
        postgres_port=os.environ.get("POSTGRES_TEST_PORT", "5432"),
        postgres_user=os.environ.get("POSTGRES_USER", "bertsonal"),
        postgres_password=os.environ.get("POSTGRES_PASSWORD", "bertsonal_pw"),
        postgres_db=os.environ.get("POSTGRES_DB", "bertsonal"),
    )

    business_date = date(2099, 12, 30)
    week_start = _week_start_date(business_date)
    month_start = _month_start_date(business_date)

    calculated_at = datetime(2099, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    # Two users, deterministic UUIDs (as plain strings, matching Spark row types).
    s1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    s2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    daily_rows = [
        (
            business_date,
            "idem_user_1",
            s1,
            "R001",
            1,
            1.0,
            0.8,
            1.0,
            0.8,
            1,
            calculated_at,
        ),
        (
            business_date,
            "idem_user_2",
            s2,
            "R001",
            2,
            1.5,
            1.2,
            1.5,
            1.2,
            2,
            calculated_at,
        ),
    ]

    metrics_rows = [
        (
            business_date,
            "R001",
            "ari",
            2,
            3,
            1.5,
            calculated_at,
        )
    ]

    word_rows = [
        (
            business_date,
            s1,
            "idem_user_1",
            "R001",
            "lari",
            0,
            1.0,
            1.0,
            0.8,
            calculated_at,
        ),
        (
            business_date,
            s2,
            "idem_user_2",
            "R001",
            "lari",
            1,
            0.5,
            1.5,
            1.2,
            calculated_at,
        ),
        (
            business_date,
            s2,
            "idem_user_2",
            "R001",
            "moro",
            0,
            1.0,
            1.5,
            1.2,
            calculated_at,
        ),
    ]

    conn = _connect_or_skip()
    try:
        # First run
        _write_gold_tables(business_date, daily_rows, metrics_rows, word_rows, cfg)

        with conn.cursor() as cur:
            snap1_daily = _fetchall(
                cur,
                """
                SELECT business_date, user_id, session_id, rhyme_id, valid_word_count,
                       daily_score, hardness_weighted_daily_score, originality_score, final_score, rank_position
                FROM gold.daily_scores
                WHERE business_date = %s
                ORDER BY user_id
                """,
                (business_date,),
            )
            snap1_rhyme = _fetchall(
                cur,
                """
                SELECT business_date, rhyme_id, rhyme, participants, total_valid_words, average_valid_words_per_session
                FROM gold.rhyme_daily_metrics
                WHERE business_date = %s
                """,
                (business_date,),
            )
            snap1_words = _fetchall(
                cur,
                """
                SELECT business_date, session_id, user_id, rhyme_id, normalized_word,
                       daily_repetitions, daily_word_score, daily_score, hardness_weighted_daily_score
                FROM gold.session_word_metrics
                WHERE business_date = %s
                ORDER BY session_id, normalized_word
                """,
                (business_date,),
            )
            snap1_week = _fetchall(
                cur,
                """
                SELECT week_start_date, user_id, total_score, average_score, days_played, rank_by_total, rank_by_average
                FROM gold.weekly_rankings
                WHERE week_start_date = %s
                ORDER BY user_id
                """,
                (week_start,),
            )
            snap1_month = _fetchall(
                cur,
                """
                SELECT month_start_date, user_id, total_score, average_score, days_played, rank_by_total, rank_by_average
                FROM gold.monthly_rankings
                WHERE month_start_date = %s
                ORDER BY user_id
                """,
                (month_start,),
            )

        # Release the read transaction so the next _write_gold_tables DDL is not blocked.
        conn.rollback()

        # Second run (same inputs)
        _write_gold_tables(business_date, daily_rows, metrics_rows, word_rows, cfg)

        with conn.cursor() as cur:
            snap2_daily = _fetchall(cur, """
                SELECT business_date, user_id, session_id, rhyme_id, valid_word_count,
                       daily_score, hardness_weighted_daily_score, originality_score, final_score, rank_position
                FROM gold.daily_scores
                WHERE business_date = %s
                ORDER BY user_id
                """, (business_date,))
            snap2_rhyme = _fetchall(cur, """
                SELECT business_date, rhyme_id, rhyme, participants, total_valid_words, average_valid_words_per_session
                FROM gold.rhyme_daily_metrics
                WHERE business_date = %s
                """, (business_date,))
            snap2_words = _fetchall(cur, """
                SELECT business_date, session_id, user_id, rhyme_id, normalized_word,
                       daily_repetitions, daily_word_score, daily_score, hardness_weighted_daily_score
                FROM gold.session_word_metrics
                WHERE business_date = %s
                ORDER BY session_id, normalized_word
                """, (business_date,))
            snap2_week = _fetchall(cur, """
                SELECT week_start_date, user_id, total_score, average_score, days_played, rank_by_total, rank_by_average
                FROM gold.weekly_rankings
                WHERE week_start_date = %s
                ORDER BY user_id
                """, (week_start,))
            snap2_month = _fetchall(cur, """
                SELECT month_start_date, user_id, total_score, average_score, days_played, rank_by_total, rank_by_average
                FROM gold.monthly_rankings
                WHERE month_start_date = %s
                ORDER BY user_id
                """, (month_start,))

        conn.rollback()

        assert snap1_daily == snap2_daily
        assert snap1_rhyme == snap2_rhyme
        assert snap1_words == snap2_words
        assert snap1_week == snap2_week
        assert snap1_month == snap2_month

        assert len(snap2_daily) == 2
        assert len(snap2_rhyme) == 1
        assert len(snap2_words) == 3
    finally:
        # Best-effort cleanup of test scopes.
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM gold.daily_scores WHERE business_date=%s", (business_date,))
                    cur.execute("DELETE FROM gold.rhyme_daily_metrics WHERE business_date=%s", (business_date,))
                    cur.execute("DELETE FROM gold.session_word_metrics WHERE business_date=%s", (business_date,))
                    cur.execute("DELETE FROM gold.weekly_rankings WHERE week_start_date=%s", (week_start,))
                    cur.execute("DELETE FROM gold.monthly_rankings WHERE month_start_date=%s", (month_start,))
        finally:
            conn.close()
