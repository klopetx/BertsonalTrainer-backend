# Simulator Design and CLI Expectations

The simulator produces synthetic session events for the BertsonalTrainer backend. It must remain deterministic when seeded, configurable for multiple workloads, and aligned with the Kafka contract.

## Purpose and runtime

- Implemented in Python 3.11 and executed via Bash-friendly CLI wrappers (see `./scripts/simulate.sh`).
- Publishes exactly one Kafka message per completed session, adhering to the canonical `session-events` contract in `docs/kafka-contract.md`.
- Remains independent of Spark; responsibility is limited to generating events and pushing them to Kafka.

## Workload profiles

- Demo: 20 users/day.
- Development: 500 users/day.
- Nominal TFM scenario: 1,000 users/day.
- Load test: 60,000 users/day (accelerated; does not need to last a real day).

## Core behavior

- Generate at most one completed session per simulated user and `business_date`.
- Support deterministic/reproducible runs via a random seed parameter.
- Allow an optional publishing delay for demo readability and near-zero delay for load tests.
- Support valid empty sessions via a configurable empty-session probability.
- For non-empty sessions, sample the submitted-word count from user-provided mean and standard deviation parameters. Results must be positive integers (`>= 1`); empty sessions originate only from the explicit empty-session probability.
- Under normal generation, select words from `data/reference/ordered_basque_dictionary.csv`; the `--rhyme` CLI option must match an `Ending` value and only words tied to that ending are eligible.
- Do not intentionally skew word selection to fabricate originality distributions.
- Provide independent probabilities for:
  - Replacing a dictionary word with a random, non-dictionary token.
  - Producing a misspelled/typo variant of a dictionary word.
- Do not lock in default percentages for empty sessions, random words, typo rates, or word-count distribution parameters without user approval; keep them configurable.

## CLI contract

`./scripts/simulate.sh` forwards the supported CLI options to the Python entrypoint (conceptually `simulator.cli`). Parameters include:

- `--users` — number of simulated players.
- `--business-date` — logical date to embed in events.
- `--rhyme` / `--rhyme-id` — daily rhyme scenario.
- `--seed` — RNG seed for reproducibility.
- `--delay-ms` (or similar) — optional inter-event delay for demos.
- `--words-mean` / `--words-stddev` — parameters for positive word-count sampling when a session is non-empty.
- `--empty-session-rate`, `--random-word-rate`, `--typo-rate` — configurable error injection knobs.

Exact option names may evolve as long as the semantics remain equivalent and the Bash wrapper forwards them transparently.

## Dictionary reference

- Runtime lexical reference: `data/reference/ordered_basque_dictionary.csv` (mounted read-only into services that need it).
- Tests use `tests/fixtures/ordered_dictionary.csv`, a deterministic subset that must not depend on the full dictionary.

## Output contract

- Emit the canonical Kafka v1 payload without applying Spark’s normalization logic. `submitted_words` must remain as typed by the simulator so the downstream normalization pipeline can run consistently in streaming and batch.
