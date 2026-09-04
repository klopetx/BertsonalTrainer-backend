from __future__ import annotations

import argparse
import os
import logging
import sys
import time
from datetime import date, datetime, timedelta
from typing import Sequence

from zoneinfo import ZoneInfo

from bertsonal_spark.batch.main import (
    CutoffNotReachedError,
    parse_business_date,
    run_batch_for_date,
)
from bertsonal_spark.config import BatchConfig


logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple polling-based scheduler for the Spark batch job.")
    parser.add_argument("--business-date", help="Run exactly once for the specified YYYY-MM-DD business date")
    parser.add_argument("--force", action="store_true", help="Bypass cutoff guardrails (testing only)")
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(float(os.getenv("SCHEDULER_POLL_INTERVAL_SECONDS", "300"))),
        help="Polling interval when running continuously (default: env or 300 seconds)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run at most one attempt for the derived business date before exiting",
    )
    return parser.parse_args(argv)


def _target_business_date(timezone: str) -> date:
    tz = ZoneInfo(timezone)
    return datetime.now(tz).date() - timedelta(days=1)


def _run_once(target_date: date, force: bool, config: BatchConfig) -> None:
    try:
        changed = run_batch_for_date(target_date, force=force, config=config)
        if not changed:
            logger.info("No rows processed for %s", target_date)
    except CutoffNotReachedError as exc:
        logger.info(str(exc))
    except Exception:  # pragma: no cover - defensive
        logger.exception("Scheduler-triggered batch failed for %s", target_date)


def _continuous_loop(args: argparse.Namespace, config: BatchConfig) -> None:
    last_processed: date | None = None
    while True:
        target = _target_business_date(config.business_timezone)
        if last_processed == target and not args.force:
            time.sleep(args.poll_interval_seconds)
            continue

        _run_once(target, force=args.force, config=config)
        last_processed = target

        if args.once:
            break

        time.sleep(args.poll_interval_seconds)


def main(argv: Sequence[str] | None = None) -> int:
    _setup_logging()
    args = parse_args(argv)
    config = BatchConfig()

    if args.business_date:
        target_date = parse_business_date(args.business_date)
        _run_once(target_date, force=args.force, config=config)
        return 0

    _continuous_loop(args, config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
