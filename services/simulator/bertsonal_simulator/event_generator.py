from __future__ import annotations

import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from .types import SimulatorConfig
from .word_repository import WordRepository


SCHEMA_VERSION = "1.0"


def _random_word_count(rng: random.Random, mean: float, stddev: float) -> int:
    if stddev <= 0:
        return max(1, int(round(mean)))
    sampled = int(round(rng.gauss(mean, stddev)))
    return max(1, sampled)


def _random_token(rng: random.Random, length: int | None = None) -> str:
    length = length or rng.randint(4, 8)
    return "".join(rng.choice(string.ascii_lowercase) for _ in range(length))


def _apply_typo(rng: random.Random, word: str) -> str:
    if not word:
        return word
    index = rng.randrange(len(word))
    replacement = rng.choice(string.ascii_lowercase)
    return f"{word[:index]}{replacement}{word[index + 1:]}"


def _simulate_words(
    rng: random.Random,
    word_repo: WordRepository,
    config: SimulatorConfig,
) -> List[str]:
    if rng.random() < config.empty_session_rate:
        return []

    count = _random_word_count(rng, config.words_mean, config.words_stddev)
    rhyme_suffix = config.rhyme
    words: List[str] = []

    for _ in range(count):
        base_word = word_repo.sample(rng, rhyme_suffix)
        roll = rng.random()
        if roll < config.random_word_rate:
            words.append(_random_token(rng))
        elif roll < config.random_word_rate + config.typo_rate:
            words.append(_apply_typo(rng, base_word))
        else:
            words.append(base_word)

    return words


def _random_session_window(rng: random.Random, business_date) -> tuple[datetime, datetime]:
    day_start = datetime.combine(business_date, datetime.min.time(), tzinfo=timezone.utc)
    start_offset = timedelta(seconds=rng.uniform(0, 23 * 3600))
    started_at = day_start + start_offset
    duration = timedelta(seconds=rng.uniform(30, 900))
    completed_at = started_at + duration
    return started_at, completed_at


def generate_event(
    rng: random.Random,
    word_repo: WordRepository,
    config: SimulatorConfig,
    user_id: str,
) -> Dict:
    submitted_words = _simulate_words(rng, word_repo, config)
    started_at, completed_at = _random_session_window(rng, config.business_date)

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "user_id": user_id,
        "business_date": config.business_date.isoformat(),
        "rhyme_id": config.rhyme_id,
        "rhyme": config.rhyme,
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
        "submitted_words": submitted_words,
    }
