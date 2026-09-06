from __future__ import annotations

import csv

from pathlib import Path
from typing import Set


def load_dictionary(path: Path) -> Set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Dictionary file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        first_line = handle.readline()
        handle.seek(0)
        if first_line.lower().startswith("ending,"):
            words = _load_csv_dictionary(handle, path)
        else:
            words = _load_plain_dictionary(handle)

    if not words:
        raise ValueError(f"Dictionary file {path} contains no usable entries")

    return words


def _load_plain_dictionary(handle) -> Set[str]:
    words: Set[str] = set()
    for raw_line in handle:
        word = raw_line.strip()
        if not word or word.startswith("#"):
            continue
        words.add(word.lower())
    return words


def _load_csv_dictionary(handle, path: Path) -> Set[str]:
    words: Set[str] = set()
    reader = csv.DictReader(handle)
    if not reader.fieldnames or {"Ending", "Word"}.difference(reader.fieldnames):
        raise ValueError(
            f"Dictionary CSV {path} must contain 'Ending' and 'Word' columns"
        )

    for row in reader:
        word = (row.get("Word") or "").strip()
        if not word:
            continue
        words.add(word.lower())

    return words
