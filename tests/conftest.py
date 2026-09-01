from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SIMULATOR_SRC = ROOT / "services" / "simulator"

if str(SIMULATOR_SRC) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_SRC))


@pytest.fixture
def fixture_dictionary_path(tmp_path: Path) -> Path:
    """Copy the test dictionary into a temp path to keep tests isolated."""

    src = Path(__file__).parent / "fixtures" / "basque_words.txt"
    dest = tmp_path / "words.txt"
    dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return dest
