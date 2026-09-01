from pathlib import Path

import pytest

from bertsonal_simulator.word_repository import WordRepository


def test_word_repository_loads_words(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)
    assert repo.words == [
        "abestia",
        "amets",
        "lagun",
        "maite",
        "oinatz",
        "poeta",
        "txakur",
    ]


def test_word_repository_filters_by_rhyme(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)
    rng = __import__("random").Random(42)

    # Words ending with "ts" should narrow the candidate list to ["oinatz"].
    sample = repo.sample(rng, rhyme_suffix="tz")
    assert sample == "oinatz"


def test_word_repository_falls_back_when_no_rhyme_match(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)
    rng = __import__("random").Random(0)

    sample = repo.sample(rng, rhyme_suffix="zzz")
    assert sample in repo.words


def test_word_repository_raises_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        WordRepository(missing)
