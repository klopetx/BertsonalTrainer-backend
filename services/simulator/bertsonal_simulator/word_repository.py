from __future__ import annotations

import logging
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List


logger = logging.getLogger(__name__)


class WordRepository:
    """Loads and samples dictionary words for the simulator."""

    def __init__(self, dictionary_path: Path) -> None:
        self.dictionary_path = dictionary_path
        self.words_by_ending = self._load_words(dictionary_path)
        self.total_words = sum(len(words) for words in self.words_by_ending.values())

    @staticmethod
    def _load_words(dictionary_path: Path) -> Dict[str, List[str]]:
        if not dictionary_path.exists():
            raise FileNotFoundError(f"Reference dictionary not found: {dictionary_path}")

        mapping: Dict[str, List[str]] = defaultdict(list)
        with dictionary_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or {"Ending", "Word"}.difference(reader.fieldnames):
                raise ValueError(
                    "Dictionary CSV must contain 'Ending' and 'Word' columns"
                )

            for row in reader:
                ending_raw = (row.get("Ending") or "").strip()
                word = (row.get("Word") or "").strip()
                if not ending_raw or not word:
                    continue
                mapping[ending_raw.lower()].append(word)

        if not mapping:
            raise ValueError(f"Dictionary at {dictionary_path} contains no usable entries")

        logger.info(
            "Loaded %s endings / %s words from %s",
            len(mapping),
            sum(len(words) for words in mapping.values()),
            dictionary_path,
        )
        return dict(mapping)

    def sample(self, rng, rhyme_suffix: str | None = None) -> str:
        """Return a random word for the requested rhyme ending."""

        if not rhyme_suffix or not rhyme_suffix.strip():
            raise ValueError("Rhyme ending is required to sample dictionary words")

        key = rhyme_suffix.strip().lower()
        candidates = self.words_by_ending.get(key)
        if not candidates:
            raise ValueError(
                f"No dictionary entries found for ending '{rhyme_suffix}' in {self.dictionary_path}"
            )

        return rng.choice(candidates)
