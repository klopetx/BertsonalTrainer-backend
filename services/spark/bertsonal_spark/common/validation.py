from __future__ import annotations

import unicodedata
from typing import Optional, Set


def normalize_token(token: Optional[str]) -> Optional[str]:
    if token is None:
        return None
    cleaned = unicodedata.normalize("NFKC", token.strip().lower())
    return cleaned if cleaned else None


def is_dictionary_word(word: Optional[str], dictionary: Set[str]) -> bool:
    if word is None:
        return False
    return word in dictionary


def matches_rhyme(word: Optional[str], rhyme: Optional[str]) -> bool:
    if not word or not rhyme:
        return False
    normalized_rhyme = normalize_token(rhyme)
    if not normalized_rhyme:
        return False
    return word.endswith(normalized_rhyme)
