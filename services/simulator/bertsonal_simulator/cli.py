from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from datetime import date
from pathlib import Path

from .event_generator import generate_event
from .kafka_client import KafkaPublisher
from .types import KafkaSettings, SimulatorConfig
from .word_repository import WordRepository


DEFAULT_DICT_PATH = "/opt/app/data/reference/ordered_basque_dictionary.csv"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BertsonalTrainer session simulator")
    parser.add_argument("--users", type=int, required=True, help="Number of simulated users")
    parser.add_argument("--business-date", type=_parse_date, required=True, help="Business date (YYYY-MM-DD)")
    parser.add_argument("--rhyme", type=str, required=True, help="Rhyme text for the day")
    parser.add_argument("--rhyme-id", type=str, required=True, help="Rhyme identifier")
    parser.add_argument("--seed", type=int, default=1234, help="Random seed for deterministic runs")
    parser.add_argument("--delay-ms", type=int, default=0, help="Optional delay between events (ms)")
    parser.add_argument("--words-mean", type=float, default=12.0, help="Average word count per session")
    parser.add_argument("--words-stddev", type=float, default=3.0, help="Word count standard deviation")
    parser.add_argument("--empty-session-rate", type=float, default=0.05, help="Probability of empty submissions")
    parser.add_argument("--random-word-rate", type=float, default=0.02, help="Probability of random token injection")
    parser.add_argument("--typo-rate", type=float, default=0.05, help="Probability of typo injection")
    return parser


def _load_env_settings() -> tuple[KafkaSettings, Path]:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "session-events")
    dictionary_path = Path(os.getenv("REFERENCE_DICT", DEFAULT_DICT_PATH))
    return KafkaSettings(kafka_bootstrap, kafka_topic), dictionary_path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)

    kafka_settings, dictionary_path = _load_env_settings()
    config = SimulatorConfig(
        users=args.users,
        business_date=args.business_date,
        rhyme=args.rhyme,
        rhyme_id=args.rhyme_id,
        words_mean=args.words_mean,
        words_stddev=args.words_stddev,
        empty_session_rate=args.empty_session_rate,
        random_word_rate=args.random_word_rate,
        typo_rate=args.typo_rate,
        delay_ms=args.delay_ms,
    )

    logger = logging.getLogger("simulator")
    logger.info(
        "Starting simulator for %s users on %s -> topic %s",
        config.users,
        config.business_date,
        kafka_settings.topic,
    )

    word_repo = WordRepository(dictionary_path)
    rng = random.Random(args.seed)
    publisher = KafkaPublisher(kafka_settings.bootstrap_servers, kafka_settings.topic)

    try:
        for user_index in range(config.users):
            user_id = f"user_{user_index + 1:05d}"
            event = generate_event(rng, word_repo, config, user_id)
            publisher.publish(key=user_id, value=event)
            if config.delay_ms > 0:
                time.sleep(config.delay_ms / 1000)
    finally:
        publisher.close()

    logger.info("Finished publishing %s events", config.users)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
