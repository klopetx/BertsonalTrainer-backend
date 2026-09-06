from pathlib import Path

import pytest

from bertsonal_simulator.word_repository import WordRepository


def test_word_repository_loads_words(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)

    assert set(repo.words_by_ending.keys()) == {"ari", "ama", "tz"}
    assert repo.words_by_ending["ari"] == ["abari", "lekari"]
    assert repo.words_by_ending["ama"] == ["panorama", "drama"]
    assert repo.words_by_ending["tz"] == ["oinatz", "berrituz"]


def test_word_repository_samples_only_matching_rhyme(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)
    rng = __import__("random").Random(42)

    samples = {repo.sample(rng, rhyme_suffix="tz") for _ in range(10)}
    assert samples.issubset(set(repo.words_by_ending["tz"]))
    assert samples  # ensure we actually sampled something


def test_word_repository_raises_when_rhyme_missing(fixture_dictionary_path: Path) -> None:
    repo = WordRepository(fixture_dictionary_path)
    rng = __import__("random").Random(0)

    with pytest.raises(ValueError):
        repo.sample(rng, rhyme_suffix="zzz")

    with pytest.raises(ValueError):
        repo.sample(rng, rhyme_suffix=None)


def test_word_repository_raises_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        WordRepository(missing)
