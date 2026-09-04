from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from bertsonal_spark.batch import main as batch_main
from bertsonal_spark.config import BatchConfig


def test_parse_business_date_success():
    result = batch_main.parse_business_date("2026-09-03")
    assert result == date(2026, 9, 3)


def test_parse_business_date_invalid():
    with pytest.raises(argparse.ArgumentTypeError):
        batch_main.parse_business_date("2026/09/03")


def test_cutoff_guard_allows_after_threshold():
    cfg = BatchConfig(business_timezone="UTC", business_cutoff_hour=1)
    target = date(2026, 9, 3)
    after_cutoff = datetime(2026, 9, 4, 1, 30, tzinfo=ZoneInfo("UTC"))
    batch_main._ensure_cutoff_passed(target, cfg, force=False, now=after_cutoff)


def test_cutoff_guard_blocks_before_threshold():
    cfg = BatchConfig(business_timezone="UTC", business_cutoff_hour=1)
    target = date(2026, 9, 3)
    before_cutoff = datetime(2026, 9, 4, 0, 30, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(batch_main.CutoffNotReachedError):
        batch_main._ensure_cutoff_passed(target, cfg, force=False, now=before_cutoff)
