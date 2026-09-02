from __future__ import annotations

from pathlib import Path
from typing import Set


def load_dictionary(path: Path) -> Set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Dictionary file not found: {path}")

    words: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            word = raw_line.strip()
            if not word or word.startswith("#"):
                continue
            words.add(word.lower())

    if not words:
        raise ValueError(f"Dictionary file {path} contains no usable entries")

    return words
