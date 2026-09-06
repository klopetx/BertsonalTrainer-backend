from __future__ import annotations

import os
from datetime import date

from bertsonal_simulator.cli import build_parser, _load_env_settings


def test_cli_parses_arguments() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--users",
            "10",
            "--business-date",
            "2026-09-01",
            "--rhyme",
            "ama",
            "--rhyme-id",
            "daily-ama",
            "--seed",
            "999",
            "--delay-ms",
            "100",
            "--words-mean",
            "15",
            "--words-stddev",
            "2",
            "--empty-session-rate",
            "0.1",
            "--random-word-rate",
            "0.2",
            "--typo-rate",
            "0.3",
        ]
    )

    assert args.users == 10
    assert args.business_date == date(2026, 9, 1)
    assert args.rhyme == "ama"
    assert args.rhyme_id == "daily-ama"
    assert args.seed == 999
    assert args.delay_ms == 100
    assert args.words_mean == 15.0
    assert args.words_stddev == 2.0
    assert args.empty_session_rate == 0.1
    assert args.random_word_rate == 0.2
    assert args.typo_rate == 0.3


def test_load_env_settings_respects_overrides(monkeypatch, tmp_path):
    custom_dict = tmp_path / "dict.csv"
    custom_dict.write_text("Ending,Word\nari,abari\n", encoding="utf-8")

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "custom:1234")
    monkeypatch.setenv("KAFKA_TOPIC", "custom-topic")
    monkeypatch.setenv("REFERENCE_DICT", str(custom_dict))

    kafka_settings, dict_path = _load_env_settings()

    assert kafka_settings.bootstrap_servers == "custom:1234"
    assert kafka_settings.topic == "custom-topic"
    assert dict_path == custom_dict
