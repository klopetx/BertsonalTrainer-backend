from __future__ import annotations

from datetime import date

import pytest

from bertsonal_spark.batch import main as batch_main


def _make_dictionary_df(spark_session):
    return spark_session.createDataFrame(
        [("era", 10)],
        ["normalized_rhyme", "dictionary_word_count"],
    )


def test_compute_word_metrics_and_scores(spark_session):
    business_date = date(2026, 9, 6)

    accepted_df = spark_session.createDataFrame(
        [
            ("session-1", "user_1", business_date, "RERA", "era", 3),
            ("session-2", "user_2", business_date, "RERA", "era", 4),
        ],
        [
            "session_id",
            "user_id",
            "business_date",
            "rhyme_id",
            "rhyme",
            "valid_word_count",
        ],
    )

    words_df = spark_session.createDataFrame(
        [
            ("session-1", "user_1", business_date, "RERA", "alpha", True),
            ("session-2", "user_2", business_date, "RERA", "alpha", True),
            ("session-2", "user_2", business_date, "RERA", "beta", True),
        ],
        [
            "session_id",
            "user_id",
            "business_date",
            "rhyme_id",
            "normalized_word",
            "is_valid",
        ],
    )

    dict_df = _make_dictionary_df(spark_session)

    word_metrics_df, session_scores_df = batch_main._compute_word_and_session_scores(
        accepted_df, words_df, dict_df
    )

    assert word_metrics_df is not None
    assert session_scores_df is not None

    word_rows = {(row.session_id, row.normalized_word): row for row in word_metrics_df.collect()}
    assert pytest.approx(word_rows[("session-1", "alpha")].daily_repetitions, rel=1e-9) == 1
    assert pytest.approx(word_rows[("session-2", "beta")].daily_repetitions, rel=1e-9) == 0
    assert pytest.approx(word_rows[("session-1", "alpha")].daily_word_score, rel=1e-9) == 0.5
    assert pytest.approx(word_rows[("session-2", "beta")].daily_word_score, rel=1e-9) == 1.0

    session_rows = {row.session_id: row for row in session_scores_df.collect()}
    assert pytest.approx(session_rows["session-1"].daily_score, rel=1e-9) == 0.5
    assert pytest.approx(session_rows["session-2"].daily_score, rel=1e-9) == 1.5

    multiplier = 1 - (10 / 74) * 0.6
    assert pytest.approx(
        session_rows["session-1"].hardness_weighted_daily_score, rel=1e-9
    ) == pytest.approx(0.5 * multiplier, rel=1e-9)
    assert pytest.approx(
        session_rows["session-2"].hardness_weighted_daily_score, rel=1e-9
    ) == pytest.approx(1.5 * multiplier, rel=1e-9)
