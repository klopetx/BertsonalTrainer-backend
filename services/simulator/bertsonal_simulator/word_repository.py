from __future__ import annotations

import logging
from pathlib import Path
from typing import List


logger = logging.getLogger(__name__)


class WordRepository:
    """Loads and samples dictionary words for the simulator."""

    def __init__(self, dictionary_path: Path) -> None:
        self.dictionary_path = dictionary_path
        self.words = self._load_words(dictionary_path)

    @staticmethod
    def _load_words(dictionary_path: Path) -> List[str]:
        if not dictionary_path.exists():
            raise FileNotFoundError(f"Reference dictionary not found: {dictionary_path}")

        words: List[str] = []
        with dictionary_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                words.append(line)

        if not words:
            raise ValueError(f"Dictionary at {dictionary_path} contains no usable words")

        logger.info("Loaded %s reference words from %s", len(words), dictionary_path)
        return words

    def sample(self, rng, rhyme_suffix: str | None = None) -> str:
        """Return a random word that optionally matches the rhyme suffix."""

        candidate_pool = self.words
        if rhyme_suffix:
            filtered = [word for word in self.words if word.lower().endswith(rhyme_suffix.lower())]
            if filtered:
                candidate_pool = filtered

        return rng.choice(candidate_pool)
