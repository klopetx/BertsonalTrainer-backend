from __future__ import annotations

import sys
import os
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SIMULATOR_SRC = ROOT / "services" / "simulator"
SPARK_SRC = ROOT / "services" / "spark"

paths_to_add = [SIMULATOR_SRC, SPARK_SRC]
for path in paths_to_add:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

existing_pythonpath = os.environ.get("PYTHONPATH", "")
joined_paths = os.pathsep.join([str(p) for p in paths_to_add if p])
if existing_pythonpath:
    os.environ["PYTHONPATH"] = os.pathsep.join([joined_paths, existing_pythonpath])
else:
    os.environ["PYTHONPATH"] = joined_paths


def _load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


_load_local_env()


@pytest.fixture
def fixture_dictionary_path(tmp_path: Path) -> Path:
    """Copy the test dictionary into a temp path to keep tests isolated."""

    src = Path(__file__).parent / "fixtures" / "ordered_dictionary.csv"
    dest = tmp_path / "words.txt"
    dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return dest


@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("bertsonaltrainer-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.stop()
