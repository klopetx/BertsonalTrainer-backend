from __future__ import annotations

import random
from datetime import date

import pytest

from bertsonal_simulator.event_generator import (
    generate_event,
    _simulate_words,
)
from bertsonal_simulator.types import SimulatorConfig


class StubWordRepository:
    def __init__(self, word: str = "amets") -> None:
        self.word = word
        self.calls = 0

    def sample(self, rng: random.Random, rhyme_suffix: str | None = None) -> str:
        self.calls += 1
        return self.word


def base_config(**overrides) -> SimulatorConfig:
    cfg = SimulatorConfig(
        users=1,
        business_date=date(2026, 9, 1),
        rhyme="ama",
        rhyme_id="daily-ama",
        words_mean=3.0,
        words_stddev=0.0,
        empty_session_rate=0.0,
        random_word_rate=0.0,
        typo_rate=0.0,
        delay_ms=0,
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def test_generate_event_shapes_payload() -> None:
    rng = random.Random(123)
    repo = StubWordRepository("lagun")
    event = generate_event(rng, repo, base_config(), user_id="user_00001")

    assert event["schema_version"] == "1.0"
    assert event["user_id"] == "user_00001"
    assert event["business_date"] == "2026-09-01"
    assert event["rhyme"] == "ama"
    assert len(event["submitted_words"]) == 3
    assert all(word == "lagun" for word in event["submitted_words"])
    assert event["started_at"].endswith("Z")
    assert event["completed_at"].endswith("Z")


def test_simulate_words_can_return_empty_session(monkeypatch) -> None:
    rng = random.Random(7)
    repo = StubWordRepository()
    cfg = base_config(empty_session_rate=1.0)

    words = _simulate_words(rng, repo, cfg)
    assert words == []


def test_simulate_words_injects_random_tokens(monkeypatch) -> None:
    rng = random.Random(5)
    repo = StubWordRepository("txakur")
    cfg = base_config(random_word_rate=1.0)

    monkeypatch.setattr(
        "bertsonal_simulator.event_generator._random_token",
        lambda rng, length=None: "random-token",
    )

    words = _simulate_words(rng, repo, cfg)
    assert words == ["random-token", "random-token", "random-token"]


def test_simulate_words_applies_typos(monkeypatch) -> None:
    rng = random.Random(9)
    repo = StubWordRepository("maite")
    cfg = base_config(typo_rate=1.0, random_word_rate=0.0)

    monkeypatch.setattr(
        "bertsonal_simulator.event_generator._apply_typo",
        lambda rng, word: f"{word}-typo",
    )

    words = _simulate_words(rng, repo, cfg)
    assert words == ["maite-typo", "maite-typo", "maite-typo"]
