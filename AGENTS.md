# BertsonalTrainer-backend

## Purpose

This repository contains the backend/data platform for **BertsonalTrainer**, a Master's Thesis project in Big Data & Data Engineering.

The system simulates users completing one daily rhyme exercise, ingests one event per completed session, calculates a provisional score in near real time, persists raw and curated data in a Medallion-style data lake, and later computes final scores and rankings using daily batch processing.

The project is intentionally focused on **data engineering**. Do not add product-facing APIs, frontend applications, or unrelated platform components unless explicitly approved by the user.

## Working agreement with the user

- Do not assume functional, architectural, infrastructure, or scope decisions that are not documented here.
- When a decision can materially affect architecture, contracts, infrastructure, scoring, reproducibility, or TFM deliverables, present the alternatives and ask for confirmation before implementation.
- Prefer simple, explainable choices suitable for a Master's Thesis and a local development environment.
- Avoid adding complexity only to mimic production-scale systems.
- Preserve a clear distinction between agreed decisions, recommendations, and pending decisions.
- Keep implementation traceable to the TFM goals and tutor feedback.

## Repository

- Repository name: `BertsonalTrainer-backend`
- Single repository.
- No custom OpenCode agents for now.
- `AGENTS.md` is the only OpenCode-specific project instruction file currently required.

## Reference execution environment

- Host OS: Windows.
- Container engine: Podman.
- Local orchestration: Compose.
- Main compose file should be named `compose.yaml`.
- Reference command: `podman compose up -d`.
- Target machine: 16 GB RAM.
- Recommended free disk before running the full stack: approximately 20-25 GB.
- The complete MVP runs locally.
- Cloud deployment is outside the MVP.
- Do not depend on Docker Desktop-only functionality.
- Resource usage must be appropriate for a single development workstation.

## Initial technology stack

- Python: **3.10** for Python application code, simulator, and project tooling.
- Python dependency isolation: standard-library `venv`; do not introduce Poetry, uv, Conda, or another package manager unless explicitly approved later.
- Kafka: event broker.
- Apache Spark Structured Streaming: near-real-time ingestion, validation, normalization, Silver generation, and provisional scoring.
- Apache Spark batch: daily final scoring and aggregations.
- MinIO: Bronze and Silver data lake storage.
- PostgreSQL: serving and Gold relational storage.
- Metabase: final BI layer, lower priority.
- Compose: local service orchestration.
- Cron: lightweight scheduling for the single daily batch job.

Do not add an HTTP/REST API to the MVP unless explicitly approved later.

## Python environments and container-image constraints

- Python runtime version is fixed to **3.10**.
- Local Python environments use `python -m venv`; keep virtual environments out of Git.
- The application is container-first: runtime execution is expected through Compose services rather than depending on a host Python installation.
- Keep custom runtime images small. Prefer a slim Python 3.10 base for Python-only services where compatible with their dependencies.
- Do not install development/test tooling in production/runtime image layers unless that service genuinely needs it at runtime.
- Install only the dependency set required by each service; do not make every custom image carry the full project's Python dependency set.
- Prefer official upstream images for infrastructure components such as Kafka, MinIO, PostgreSQL, and Spark instead of rebuilding those platforms from generic Python images.
- Avoid unnecessary OS packages, package-manager caches, compiler toolchains, and duplicate dependency layers in final runtime images. Multi-stage builds are allowed when native/build dependencies would otherwise make a runtime image materially larger.
- Compose remains the integration boundary: services must be independently buildable/runnable and communicate through declared network endpoints and persistent volumes.
- Dependency files are split by execution concern so each custom image installs only what it needs. Approved initial layout:

```text
requirements/
|-- simulator.txt
|-- spark.txt
`-- dev.txt
```

- `requirements/simulator.txt`: dependencies required only by the Kafka event simulator.
- `requirements/spark.txt`: Python dependencies required by the Spark streaming and batch application code. Spark/JVM platform dependencies themselves should come from the chosen Spark runtime image where practical rather than being duplicated unnecessarily through pip.
- `requirements/dev.txt`: host/development-only tooling such as tests, linting, and related quality tools; do not install this file into normal runtime images.
- Do not collapse these files into one project-wide runtime requirements file unless explicitly approved later. Shared dependencies may be duplicated between the small service-specific files if that keeps image boundaries simple; introduce a shared/base requirements file only if repetition becomes materially inconvenient.


## Repository scaffolding and execution interface

The repository is a monorepo of containerized components. Keep application code, infrastructure configuration, human-facing scripts, tests, and documentation clearly separated. Do not add API, frontend, notebook, Airflow/Prefect/Dagster, or other unrelated top-level application areas unless scope is explicitly changed.

Approved initial repository layout:

```text
BertsonalTrainer-backend/
|-- AGENTS.md
|-- README.md
|-- compose.yaml
|-- .env.example
|-- .gitignore
|-- .dockerignore
|
|-- requirements/
|   |-- simulator.txt
|   |-- spark.txt
|   `-- dev.txt
|
|-- contracts/
|   `-- session-event-v1.schema.json
|
|-- data/
|   `-- reference/
|       `-- basque_words.txt
|
|-- services/
|   |-- simulator/
|   |   |-- Dockerfile
|   |   `-- simulator/
|   |       |-- __init__.py
|   |       |-- cli.py
|   |       |-- generator.py
|   |       |-- producer.py
|   |       `-- config.py
|   |
|   `-- spark/
|       |-- Dockerfile
|       `-- bertsonal_spark/
|           |-- __init__.py
|           |-- common/
|           |   |-- config.py
|           |   |-- normalization.py
|           |   |-- validation.py
|           |   `-- postgres.py
|           |-- streaming/
|           |   |-- main.py
|           |   |-- transformations.py
|           |   `-- sinks.py
|           `-- batch/
|               |-- main.py
|               |-- deduplication.py
|               |-- metrics.py
|               |-- scoring.py
|               `-- rankings.py
|
|-- infra/
|   |-- kafka/
|   |   `-- create-topics.sh
|   |-- minio/
|   |   `-- init.sh
|   |-- postgres/
|   |   `-- init/
|   |       |-- 001-schemas.sql
|   |       |-- 002-serving.sql
|   |       `-- 003-gold.sql
|   `-- scheduler/
|       `-- crontab
|
|-- scripts/
|   |-- infra-up.sh
|   |-- streaming-up.sh
|   |-- simulate.sh
|   |-- batch-run.sh
|   |-- scheduler-up.sh
|   |-- logs.sh
|   |-- test.sh
|   `-- down.sh
|
|-- tests/
|   |-- unit/
|   |   |-- simulator/
|   |   `-- spark/
|   |-- integration/
|   |-- performance/
|   `-- fixtures/
|       `-- basque_words.txt
|
`-- docs/
    `-- acceptance-criteria.md
```

Repository-boundary rules:

- `services/` contains project-owned application code.
- `infra/` contains configuration/bootstrap assets for external platforms such as Kafka, MinIO, PostgreSQL, and cron.
- `scripts/` is the supported human-facing Bash-compatible execution interface for local development and the recorded demo. Scripts may wrap Compose commands but should keep those details out of the normal user workflow.
- `tests/` separates unit, integration, performance, and deterministic fixture data.
- `contracts/session-event-v1.schema.json` is the versioned machine-readable Kafka event contract and must remain consistent with both producer and consumer behavior.
- `data/reference/basque_words.txt` is the intended runtime lexical reference path. `tests/fixtures/basque_words.txt` is a small deterministic test fixture and must not depend on the full runtime dictionary.
- Mount the runtime lexical reference read-only into services that need it where practical instead of duplicating the large file into multiple custom images.

### Simulator service structure

Keep the simulator independent of Spark:

- `generator.py`: deterministic session/word generation logic.
- `producer.py`: Kafka publication logic.
- `cli.py`: command-line interface and argument parsing.
- `config.py`: environment/configuration handling.

Its runtime image should remain Python-only and lean, conceptually `python:3.10-slim + requirements/simulator.txt + simulator code`, subject to actual dependency compatibility.

### Spark application structure

Use one project-owned Spark codebase and one reusable Spark runtime image for both streaming and batch. Do not create separate duplicate projects/images for these two execution modes unless a demonstrated runtime constraint requires it.

- `bertsonal_spark/common/` contains genuinely shared configuration, normalization, validation, and PostgreSQL helpers.
- `bertsonal_spark/streaming/` contains the long-running Structured Streaming entrypoint, transformations, and sinks.
- `bertsonal_spark/batch/` contains the one-shot daily batch entrypoint, definitive deduplication, metrics, scoring, and rankings.
- Compose may instantiate the same Spark image as different services/commands, notably `spark-streaming`, `spark-batch`, and the scheduler runtime described below.

### Approved Compose service names

Initial Compose service names are:

```text
kafka
kafka-init
minio
minio-init
postgres
simulator
spark-streaming
spark-batch
scheduler
```

`metabase` may be added later in P2.

Initialization rules:

- `kafka-init` is a one-shot bootstrap process that explicitly creates/configures the `session-events` topic with the agreed one-partition, replication-factor-1, 3-day-retention settings. Do not rely on implicit topic auto-creation for the reproducible project setup.
- `minio-init` is a one-shot bootstrap process for required buckets/namespaces and other agreed storage initialization.
- PostgreSQL schema/table/index initialization lives under `infra/postgres/init/` and must reproduce the approved `serving` and `gold` database structures.

### Approved manual Bash interface

The following script names are the supported local/demo interface. Keep the wrappers simple and have them invoke the real containerized implementation rather than alternate demo-only code.

Start core infrastructure:

```bash
./scripts/infra-up.sh
```

Start Structured Streaming manually:

```bash
./scripts/streaming-up.sh
```

Run the simulator, forwarding its CLI parameters:

```bash
./scripts/simulate.sh \
  --users 20 \
  --business-date 2026-08-31 \
  --rhyme "ari" \
  --seed 42
```

The simulator CLI must also expose the already-agreed configurable concepts for delay, positive non-empty word-count mean/standard deviation, empty-session rate, random non-dictionary word rate, and typo rate. Their final defaults remain a separate open decision.

Run the daily batch manually:

```bash
./scripts/batch-run.sh --business-date 2026-08-31
```

Run the same batch implementation in explicit local/test/demo force mode:

```bash
./scripts/batch-run.sh --business-date 2026-08-31 --force
```

Start the automatic scheduler when needed:

```bash
./scripts/scheduler-up.sh
```

Inspect service logs:

```bash
./scripts/logs.sh spark-streaming
```

Run project tests:

```bash
./scripts/test.sh
```

Stop the environment without deleting persistent data:

```bash
./scripts/down.sh
```

Stopping services and destroying persistent data/volumes must remain separate operations. `down.sh` must not silently delete project data.

### Scheduler runtime design

Do not give the scheduler container access to the Podman/Docker engine socket merely so cron can launch another Compose container. Avoid this unnecessary privilege and coupling.

Instead, the scheduler is a dedicated Compose service built from/reusing the Spark batch-capable runtime and executes the same `bertsonal_spark/batch/main.py` implementation through cron/spark-submit. Conceptually:

```text
manual path
spark-batch service
    -> batch/main.py

automatic path
scheduler service
    -> cron
    -> same batch/main.py
```

This keeps scheduler responsibility separate while preventing duplicate batch implementations. Reuse image layers where practical; the scheduler does not need to be a separate tiny Python image because executing Spark batch logic legitimately requires the Spark runtime.

## High-level architecture

### Normal streaming path

```text
Python simulator
      |
      v
Kafka
      |
      v
Spark Structured Streaming
      |---> MinIO Bronze
      |---> MinIO Silver
      |       |-- sessions
      |       `-- session-words
      `---> PostgreSQL serving.provisional_scores
```

### Daily batch path

```text
MinIO Silver
      |
      v
Spark Batch
      |-- originality
      |-- rhyme difficulty / weighting
      |-- final daily scores
      |-- daily rankings
      |-- weekly rankings
      `-- monthly rankings
      |
      v
PostgreSQL gold
```

### Recovery path

Bronze is the reproducible source of truth. The design must allow a future recovery/reprocessing job conceptually equivalent to:

```text
rebuild_silver_from_bronze
```

This recovery tool is not required in the first implementation milestone.

## Functional rules

- All users see the same rhyme for a given business day.
- A user can complete at most one official session per business day.
- No functional/game retry is allowed.
- Technical retries are allowed.
- A technical retry must preserve the same `event_id`, `session_id`, `user_id`, `business_date`, and payload.
- A technical retry must not create an additional logical session or an additional score.
- An identical `event_id` with different content is an integrity conflict, not a valid retry.
- Empty completed sessions are valid: `submitted_words` may be `[]` and the provisional score is `0`.

## Kafka design - v1

- Local brokers: 1.
- Main topic: `session-events`.
- Partitions: 1.
- Replication factor: 1.
- Message key: `user_id`.
- Main consumer: Spark Structured Streaming.
- Retention: 3 days.
- One Kafka message represents one completed user session.
- Kafka is transport/buffer storage, not the long-term historical store.
- MinIO Bronze is the long-term raw source of truth.

The single partition is a deliberate local-MVP simplification. The architecture may be extended to more partitions for load/scale testing later.

## Kafka event contract - v1

The canonical v1 payload is conceptually:

```json
{
  "schema_version": "1.0",
  "event_id": "uuid",
  "session_id": "uuid",
  "user_id": "string",
  "business_date": "YYYY-MM-DD",
  "rhyme_id": "string",
  "rhyme": "string",
  "started_at": "ISO-8601 UTC",
  "completed_at": "ISO-8601 UTC",
  "submitted_words": ["string"]
}
```

Rules:

- `schema_version` is mandatory and initially supports `1.0`.
- `event_id` identifies the logical event and is the primary idempotency identifier.
- `session_id` identifies the user's game session.
- `user_id` is a simulated/pseudonymous identifier and is also the Kafka key.
- `business_date` is the functional day of the exercise, not ingestion time.
- Both `rhyme_id` and `rhyme` are retained so that the event is self-contained.
- `started_at` and `completed_at` use ISO-8601 UTC.
- `submitted_words` contains the words as submitted; the producer must not apply the Spark normalization logic before sending the event.
- `submitted_words` may be empty.
- Derived values such as provisional score, validity, originality, final score, and ranking do not belong in the input event.

## Event-level validation

Before word validation, the streaming job must validate the event contract.

Examples of required checks:

- Supported `schema_version`.
- Valid `event_id`.
- Valid `session_id`.
- Non-empty `user_id`.
- Valid `business_date`.
- Non-empty `rhyme_id`.
- Non-empty `rhyme`.
- Valid timestamps.
- `completed_at >= started_at`.
- `submitted_words` is an array/list of strings.

A malformed individual event must not crash the entire streaming pipeline.

## Lexical reference data

A Basque word list will be provided later as:

```text
data/reference/basque_words.txt
```

Until the real file is available, tests may use a small fixture such as:

```text
tests/fixtures/basque_words.txt
```

The same normalization and lexical rules must be shared by simulator-related test data and Spark validation logic to avoid inconsistent behavior.

## Simulator design

The simulator is implemented in Python and must be executable from Bash. Shell scripts may provide convenient wrappers, but the simulation rules and event-generation logic belong in Python, not in Bash.

Reference workload profiles:

- Demo: 20 users/day.
- Development: 500 users/day.
- Nominal TFM scenario: 1,000 users/day.
- Load test: 60,000 users/day.

The load-test profile is an accelerated test workload; it does not need to take a real day to publish 60,000 session events.

Simulator rules:

- Generate at most one completed session per simulated user and `business_date`.
- Support deterministic/reproducible runs via a random seed.
- Support an optional publishing delay for visually understandable demos and no/low delay for load tests.
- Support valid empty sessions through a configurable empty-session probability/rate.
- For non-empty sessions, generate the number of submitted words from configurable mean and standard-deviation parameters.
- Expose these parameters conceptually as `--words-mean` and `--words-stddev` (exact CLI naming may be finalized during scaffolding).
- The generated word count for a non-empty session must always be a positive integer (`>= 1`); negative or zero samples are not valid outputs of this distribution. Empty sessions are produced only through the explicit empty-session probability/rate.
- Do not silently choose final default values for the mean or standard deviation; they remain configurable until workload assumptions are approved.
- Under normal generation, submitted words are selected from `basque_words.txt` and must be compatible with the selected daily rhyme.
- Do not artificially weight specific words to manufacture an originality distribution. Word selection may be random among eligible candidates.
- Support a configurable probability/rate for replacing a normal dictionary selection with a randomly generated token that is not sourced from the dictionary.
- Support a separate configurable probability/rate for producing a misspelled/corrupted word.
- The exact default percentages for empty sessions, random non-dictionary words, and misspellings are not yet approved. Keep them configurable and do not silently choose final project defaults.
- The simulator must emit the canonical Kafka v1 session contract and publish one Kafka message per completed session.

Bash usage is standardized through `./scripts/simulate.sh`, which forwards the simulator parameters for user count, business date/rhyme scenario, seed, delay, word-count mean/standard deviation, empty-session rate, and error-injection rates to the real simulator implementation.

## Word normalization and validation

The agreed MVP validation flow is:

```text
original word
  -> trim surrounding whitespace
  -> lowercase
  -> Unicode normalization
  -> duplicate check within session
  -> dictionary membership check
  -> rhyme suffix check
```

Do not remove accents/diacritics or language-specific characters as part of normalization unless explicitly approved later.

The MVP rhyme rule is intentionally deterministic and simplified:

```text
normalized_word ends with normalized_rhyme
```

This must be documented as an MVP simplification, not as a complete linguistic model of Basque or bertsolaritza rhyme.

Suggested stable validation reasons:

- `duplicate_in_session`
- `not_in_dictionary`
- `rhyme_mismatch`
- `empty_or_invalid_token`

## Bronze layer

- Storage: MinIO.
- Format: Parquet.
- Behavior: append-only and immutable.
- Bronze preserves every physical Kafka delivery, including technical duplicates.
- The original Kafka payload must be preserved without functional transformation.
- Recommended raw payload representation: a raw/binary payload plus Kafka metadata.

Required technical metadata should include at least:

- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `ingested_at`
- `raw_payload`

Partitioning:

```text
bronze/session-events/ingestion_date=YYYY-MM-DD/part-*.parquet
```

Bronze is partitioned by system-generated `ingestion_date`, not `business_date`.

## Silver layer

Silver is generated by Spark Structured Streaming during the normal path.

Silver represents structured, normalized, classified data. It may contain invalid words, as long as the enclosing event/session was processable.

Silver contains two datasets:

```text
silver/sessions/
silver/session-words/
```

Recommended partitioning:

```text
silver/sessions/business_date=YYYY-MM-DD/part-*.parquet
silver/session-words/business_date=YYYY-MM-DD/part-*.parquet
```

### `silver/sessions`

One row represents one processable Kafka session delivery. Silver is structured and validated, but it is not the final idempotency boundary. Technical retries may therefore produce multiple physical Silver rows when they arrive in different streaming microbatches.

Approved schema:

| Field | Type | Purpose |
|---|---|---|
| `source_record_id` | string | Technical identity of the physical Kafka delivery, derived from topic/partition/offset (for example `session-events:0:152`). |
| `schema_version` | string | Input contract version. |
| `event_id` | string | Logical event identifier used by the batch for definitive technical deduplication. |
| `payload_hash` | string | SHA-256 hash of the original Kafka payload, used to distinguish a legitimate retry from an integrity conflict. |
| `session_id` | string | Functional session identifier. |
| `user_id` | string | Simulated user identifier. |
| `business_date` | date | Logical exercise day and Silver partition key. |
| `rhyme_id` | string | Identifier of the daily rhyme. |
| `rhyme` | string | Rhyme shown to the user; retained so processed records remain self-describing. |
| `started_at` | timestamp | Session start time in UTC. |
| `completed_at` | timestamp | Session completion time in UTC. |
| `submitted_word_count` | integer | Number of submitted words, including invalid/duplicate entries. |
| `valid_word_count` | integer | Number of distinct words accepted by the agreed validation rules. |
| `invalid_word_count` | integer | Number of submitted word entries that do not contribute to provisional scoring. |
| `kafka_topic` | string | Kafka lineage metadata. |
| `kafka_partition` | integer | Kafka lineage metadata. |
| `kafka_offset` | long | Kafka lineage metadata. |
| `kafka_timestamp` | timestamp | Kafka record timestamp. |
| `processed_at` | timestamp | Timestamp at which Spark produced the Silver row. |

Rules:

- Empty sessions are retained with `submitted_word_count = 0`, `valid_word_count = 0`, and `invalid_word_count = 0`.
- A technical retry may produce another physical row with a different `source_record_id` but the same `event_id`.
- `payload_hash` must be calculated from the original Kafka payload before functional transformation.
- Same `event_id` + same `payload_hash` is compatible with a technical retry.
- Same `event_id` + different `payload_hash` is an `INTEGRITY_CONFLICT`.
- Distinct events that violate the one-session-per-user-per-business-day rule may remain physically present in Silver. The definitive rule is **first accepted event wins**: after technical-retry collapse, the earliest accepted event for a given `user_id + business_date` is the only event allowed to contribute to serving/Gold; later distinct events are business-rule violations.
- Do not duplicate the `submitted_words` array in `silver/sessions`; word-level detail is stored in `silver/session-words` and the untouched payload remains in Bronze.

### `silver/session-words`

One row represents one element of the source event's `submitted_words` array. Keep both accepted and rejected words so Silver captures the structured interpretation of the session.

Approved schema:

| Field | Type | Purpose |
|---|---|---|
| `source_record_id` | string | Links the word to the exact physical `silver/sessions` delivery. |
| `event_id` | string | Logical event identifier for batch deduplication. |
| `session_id` | string | Functional session identifier. |
| `user_id` | string | Simulated user identifier; intentionally denormalized to simplify Spark aggregations. |
| `business_date` | date | Logical day and Silver partition key. |
| `rhyme_id` | string | Identifier of the daily rhyme. |
| `word_index` | integer | Zero-based position of the item in the original `submitted_words` array. |
| `original_word` | string | Value exactly as submitted in the event. |
| `normalized_word` | string/null | Result after the approved trim/lowercase/Unicode normalization; null if no usable normalized token can be produced. |
| `is_valid` | boolean | Whether the word contributes to scoring. |
| `validation_reason` | string/null | Stable rejection reason when `is_valid = false`; null for accepted words. |

Rules:

- The first occurrence of a normalized valid word in a session may be accepted; later occurrences of the same normalized word are stored with `is_valid = false` and `validation_reason = duplicate_in_session`.
- `word_index` preserves the original array ordering and makes repeated values independently traceable.
- Only rows with `is_valid = true` contribute to provisional scoring and later Gold calculations.
- Derived metrics such as provisional score, originality, rhyme difficulty, final score, and ranking do not belong in Silver.

Technical retry duplicates are always preserved in Bronze. Silver may preserve them as structured records; definitive deduplication is performed before Gold calculations. An optional best-effort `event_id` deduplication inside an individual streaming microbatch is allowed, but the system must not depend on stateful cross-microbatch deduplication.

## Streaming state, checkpointing, and deduplication

The MVP deliberately avoids Spark watermarking and other stateful event-time deduplication mechanisms. The use case does not require streaming windows or stream-stream joins, and adding watermark-driven state management would add complexity without proportional value.

Approved behavior:

- No Spark watermark is required in the MVP.
- The business late-event cutoff (`01:00 Europe/Madrid` for the previous business day) is enforced explicitly as a business rule and is independent of Spark event-time watermarking.
- Spark Structured Streaming must still use persistent checkpointing so a stopped/restarted query can recover its Kafka progress and streaming state required by Spark itself.
- Store checkpoints in a technical MinIO namespace separate from Bronze/Silver, conceptually:

```text
system/checkpoints/streaming/<query-name>/
```

- Each independent streaming query must use its own checkpoint location.
- Do not use watermarking solely to deduplicate technical retries.
- Streaming may perform best-effort duplicate removal within the current microbatch using `event_id`, but correctness must not depend on that optimization.
- `serving.provisional_scores` is an idempotency boundary and must use database uniqueness constraints plus an idempotent write/upsert strategy.
- The daily batch is the definitive business-level deduplication step before Gold calculations. It must collapse technical retries by `event_id` and enforce one accepted logical session per `user_id + business_date`.
- When multiple distinct `event_id` values exist for the same `user_id + business_date`, **first accepted event wins**. With the v1 single-partition topic, Kafka offset provides the deterministic arrival order. Later distinct events must not overwrite the accepted session and should be classified/logged as `BUSINESS_RULE_VIOLATION` (and later routed to quarantine when that P2 capability exists).

## PostgreSQL

Use one PostgreSQL instance but separate provisional serving data from final Gold data using schemas. PostgreSQL is a serving/analytical layer, not a copy of Silver.

```text
PostgreSQL
|-- serving
|   `-- provisional_scores
`-- gold
    |-- daily_scores
    |-- rhyme_daily_metrics
    |-- weekly_rankings
    `-- monthly_rankings
```

`gold.business_kpis` is deliberately not implemented for now. Reconsider it only if the later Metabase design reveals KPIs that are not already served cleanly by the approved Gold tables.

General rules:

- Do not overwrite provisional scores with final scores.
- Provisional and final scores must remain independently auditable.
- Do not add surrogate `BIGSERIAL` identifiers when the approved natural/business keys are sufficient.
- Add only indexes justified by concrete access patterns; do not create speculative indexes.
- Streaming serving writes and batch Gold writes use different idempotency strategies, described below.

### `serving.provisional_scores`

One row per accepted user session:

| Column | PostgreSQL type | Notes |
|---|---|---|
| `event_id` | `UUID` | Logical event identifier and technical idempotency key |
| `session_id` | `UUID` | Functional session identifier |
| `user_id` | `TEXT` | Simulated user |
| `business_date` | `DATE` | Logical exercise day |
| `rhyme_id` | `TEXT` | Daily rhyme identifier |
| `valid_word_count` | `INTEGER` | Distinct valid words |
| `provisional_score` | `INTEGER` | Equal to `valid_word_count` in v1 |
| `calculated_at` | `TIMESTAMPTZ` | Streaming calculation time |

Constraints:

```sql
PRIMARY KEY (event_id)
UNIQUE (session_id)
UNIQUE (user_id, business_date)
CHECK (valid_word_count >= 0)
CHECK (provisional_score >= 0)
```

Additional index:

```sql
CREATE INDEX idx_provisional_scores_business_date
ON serving.provisional_scores (business_date);
```

The primary/unique constraints already create indexes; do not duplicate them.

Streaming write policy:

- Use insert semantics equivalent to `INSERT ... ON CONFLICT DO NOTHING`.
- A legitimate technical retry (`event_id`/payload unchanged) must not create or update an additional score.
- If a different `event_id` arrives for an already accepted `user_id + business_date`, the first accepted event remains authoritative; the later event must not overwrite it.
- When P2 quarantine exists, later distinct events for the same user/day should be classified as `BUSINESS_RULE_VIOLATION`.

### Definitive user/day conflict rule

The project uses **first accepted event wins**. Batch processing applies the same rule deterministically after collapsing technical retries:

1. Collapse repeated physical deliveries of the same logical `event_id`.
2. Detect same `event_id` with differing `payload_hash` as `INTEGRITY_CONFLICT`.
3. For remaining distinct events sharing `user_id + business_date`, accept only the earliest Kafka arrival.
4. With the v1 one-partition topic, the smallest Kafka offset is the deterministic first arrival. If partitioning is increased later, `user_id` remains the Kafka key so each user's events preserve per-user ordering within one partition.
5. Later distinct events do not contribute to provisional or final scoring.

### `gold.daily_scores`

One row per accepted user and business day after definitive batch deduplication:

| Column | PostgreSQL type | Notes |
|---|---|---|
| `business_date` | `DATE` | Logical day |
| `user_id` | `TEXT` | User |
| `session_id` | `UUID` | Accepted session |
| `rhyme_id` | `TEXT` | Daily rhyme |
| `valid_word_count` | `INTEGER` | Distinct valid words |
| `originality_score` | `NUMERIC` | Nullable until final formula is approved |
| `rhyme_difficulty` | `NUMERIC` | Derived difficulty/weight; nullable while transformation is pending |
| `final_score` | `NUMERIC` | Nullable until final scoring formula is approved |
| `rank_position` | `INTEGER` | Materialized daily rank; nullable until final score exists |
| `calculated_at` | `TIMESTAMPTZ` | Batch calculation time |

Constraints/indexes:

```sql
PRIMARY KEY (business_date, user_id)
UNIQUE (session_id)

CREATE INDEX idx_daily_scores_date_rank
ON gold.daily_scores (business_date, rank_position);
```

Materialize `rank_position` rather than recalculating it at query time.

### `gold.rhyme_daily_metrics`

One row per business day. The primary key intentionally enforces the agreed business rule that all users receive one common rhyme per day.

| Column | PostgreSQL type | Notes |
|---|---|---|
| `business_date` | `DATE` | Primary key |
| `rhyme_id` | `TEXT` | Daily rhyme identifier |
| `rhyme` | `TEXT` | Self-describing rhyme text/suffix |
| `participants` | `INTEGER` | Completed accepted sessions, including empty sessions |
| `total_valid_words` | `INTEGER` | Total valid distinct words contributed across accepted sessions |
| `average_valid_words_per_session` | `NUMERIC` | Difficulty source metric |
| `rhyme_difficulty` | `NUMERIC` | Derived transformation; nullable until formula is approved |
| `calculated_at` | `TIMESTAMPTZ` | Batch calculation time |

Constraint:

```sql
PRIMARY KEY (business_date)
```

### `gold.weekly_rankings`

Use `week_start_date` (Monday, ISO-style business week anchor) instead of a `year + week` pair.

| Column | PostgreSQL type |
|---|---|
| `week_start_date` | `DATE` |
| `user_id` | `TEXT` |
| `total_score` | `NUMERIC` |
| `average_score` | `NUMERIC` |
| `days_played` | `INTEGER` |
| `rank_by_total` | `INTEGER` |
| `rank_by_average` | `INTEGER` |
| `calculated_at` | `TIMESTAMPTZ` |

Constraints/indexes:

```sql
PRIMARY KEY (week_start_date, user_id)

CREATE INDEX idx_weekly_rank_total
ON gold.weekly_rankings (week_start_date, rank_by_total);

CREATE INDEX idx_weekly_rank_average
ON gold.weekly_rankings (week_start_date, rank_by_average);
```

`average_score` is calculated over `days_played`; days without participation do not count as zero-score days in the average.

### `gold.monthly_rankings`

Use the first calendar day of the month as `month_start_date` instead of a `year + month` pair.

| Column | PostgreSQL type |
|---|---|
| `month_start_date` | `DATE` |
| `user_id` | `TEXT` |
| `total_score` | `NUMERIC` |
| `average_score` | `NUMERIC` |
| `days_played` | `INTEGER` |
| `rank_by_total` | `INTEGER` |
| `rank_by_average` | `INTEGER` |
| `calculated_at` | `TIMESTAMPTZ` |

Constraints/indexes:

```sql
PRIMARY KEY (month_start_date, user_id)

CREATE INDEX idx_monthly_rank_total
ON gold.monthly_rankings (month_start_date, rank_by_total);

CREATE INDEX idx_monthly_rank_average
ON gold.monthly_rankings (month_start_date, rank_by_average);
```

### Gold idempotency and replacement strategy

Gold must support deterministic reprocessing. Do **not** use `ON CONFLICT DO NOTHING` as the Gold correctness mechanism because reprocessing may legitimately change the derived rows.

Use **transactional replace-by-scope** semantics:

- Recalculate the complete affected `business_date` from Silver.
- Recalculate the complete affected `week_start_date`.
- Recalculate the complete affected `month_start_date`.
- Inside a database transaction, delete the previous rows for those exact scopes and insert the newly calculated rows.
- Commit only when every affected Gold table has been written successfully; otherwise roll back.

Conceptually:

```text
BEGIN
  replace gold.daily_scores for business_date D
  replace gold.rhyme_daily_metrics for business_date D
  replace gold.weekly_rankings for week W containing D
  replace gold.monthly_rankings for month M containing D
COMMIT
```

This prevents stale rows from surviving when a reprocessing run produces fewer records than an earlier run and makes repeated execution for the same scope idempotent.

Metabase will consume PostgreSQL directly in a later phase. Direct PostgreSQL queries are sufficient during the main development stages.

## Provisional scoring

The provisional score is intentionally simple:

```text
provisional_score = number of distinct valid submitted words
```

An empty valid session has provisional score `0`.

Streaming and batch must use the same word normalization and validity criteria.

## Deferred/final scoring concepts

Do not invent a final mathematical formula until explicitly approved.

The final score will use at least these concepts:

### Originality

- Calculated after the full set of daily user responses is available.
- Only valid words are considered.
- A word is more original when fewer distinct users used that same normalized word during the same business day/rhyme.
- The final transformation from frequency to points is not yet defined.

### Rhyme difficulty

Difficulty is inferred from observed user performance, not manually assigned.

For a business day:

```text
average_valid_words_per_session =
    total distinct valid words contributed across sessions
    / total completed sessions
```

Rules:

- Count only valid, distinct words in the numerator.
- Include every completed session in the denominator, including sessions with zero valid words.
- Lower average means a harder rhyme.
- Higher average means an easier rhyme.
- The final transformation from this metric to a scoring weight is not yet defined.

### Final formula

Pending. Do not choose weights or a mathematical combination without user approval.

## Business day close and late-event policy

- Business timezone: `Europe/Madrid`.
- Event timestamps remain stored in UTC; `business_date` is interpreted using the business timezone.
- The logical game day changes at `00:00` local time.
- Previous-day sessions have a 1-hour grace window after midnight.
- The acceptance cutoff for business day `D` is therefore `01:00` local time on `D+1`.
- A previous-day event arriving after that cutoff is not accepted into the functional pipeline.
- Late events may be considered suspicious/malicious input and must not alter Silver, provisional scoring, Gold, or rankings.
- Because Bronze is the immutable ingestion/audit layer, a late Kafka delivery may still be persisted physically in Bronze with its Kafka metadata.
- When the lower-priority quarantine capability exists, late events should be classified explicitly (for example `LATE_EVENT`) instead of being silently dropped.
- Gold for a business day must not be calculated through the normal scheduled path until the 1-hour grace window has closed.
- The exact small technical delay/readiness check between the `01:00` cutoff and automatic batch start remains to be defined.

During the interval from `00:00` to `01:00`, the streaming pipeline may legitimately receive both:

- sessions for the new business day, and
- delayed sessions for the previous business day that are still within the grace window.

## Batch processing

The normal daily batch consumes Silver, not Kafka and not Bronze.

Responsibilities include:

- daily word-frequency aggregates
- originality metrics
- rhyme difficulty metrics
- final daily score
- daily ranking
- weekly ranking metrics, including cumulative and average performance
- monthly ranking metrics, including cumulative and average performance
- Gold KPI generation

The batch is scheduled by a lightweight cron-based scheduler service in Compose. The business-day acceptance cutoff is agreed. Watermarking is explicitly out of scope for the MVP; persistent streaming checkpointing is required. The small post-cutoff readiness delay/check is still pending.

## Manual execution and demo mode

The project must support explicit manual execution of the main stages. This is a functional requirement for development, testing, and recording the TFM demonstration. Do not design the system so that the user must wait for real clock time or for the automatic scheduler to demonstrate the pipeline.

Required manual capabilities:

1. **Message simulation**
   - Manually generate/publish a configurable set of synthetic completed-session events to Kafka.
   - The simulator should support deterministic runs (for example via a seed) so a demo/test can be repeated.
   - It should be possible to choose or explicitly provide the logical `business_date`/rhyme scenario used for the simulation.

2. **Spark Structured Streaming execution**
   - The streaming job must have an explicit manual start path for local development/demo.
   - It must be possible to observe the full Kafka -> Bronze/Silver -> provisional PostgreSQL path without depending on an external scheduler.
   - Manual execution must use the same production/MVP transformation logic; do not create a separate fake implementation for demos.

3. **Spark batch execution**
   - The daily batch must be manually executable for an explicit `business_date`.
   - A controlled local/demo `force` mechanism must allow the user to run the batch before the real-time day-close condition, specifically for tests and the recorded demonstration.
   - Forced execution must be explicit, clearly logged, and must not become the default behavior of the scheduled path.
   - Re-running the batch for the same date must remain idempotent.

The exact manual wrapper names and repository layout are now fixed in the `Repository scaffolding and execution interface` section above.

## Batch orchestration

The project deliberately does **not** use Airflow, Prefect, or Dagster for the MVP. There is a single scheduled daily batch, so a full workflow orchestrator would add operational complexity without proportional value.

Approved design:

- Use a lightweight cron-based scheduler.
- Run cron as a dedicated service in `compose.yaml`, separate from the long-running Spark streaming process.
- The scheduler invokes the same batch implementation used by manual/demo execution; do not maintain a separate scheduled code path.
- Normal automatic execution occurs after the previous business day's `01:00 Europe/Madrid` acceptance cutoff, with a small readiness delay/check still to be defined.
- Manual execution for an explicit `business_date` remains available.
- A `--force`-style capability is allowed only for local development, tests, and recorded demos; it must be explicit and logged.
- Re-running a scheduled or manually invoked batch for the same `business_date` must be idempotent.
- Do not embed unrelated long-running application processes into the scheduler container. Keep scheduling and Spark execution responsibilities conceptually separate.

Rationale to preserve in project documentation: the MVP has one time-triggered batch with simple dependencies, so cron is sufficient and easier to operate on the reference Windows/Podman workstation. A heavier orchestrator may be reconsidered only if the workflow graph becomes materially more complex.

A target recorded-demo flow should be possible conceptually as:

```text
start local infrastructure
    -> start Spark Structured Streaming manually
    -> simulate/publish session events manually
    -> inspect Bronze + Silver + serving.provisional_scores
    -> force daily batch for the chosen business_date
    -> inspect PostgreSQL Gold/rankings
```

## Quarantine / invalid events

A technical `quarantine` area in MinIO is approved but has lower implementation priority.

Conceptual behavior:

```text
Kafka event
  |-- always -> Bronze
  |-- valid event -> Silver + provisional score
  `-- unprocessable event -> quarantine
```

Potential error categories:

- `JSON_PARSE_ERROR`
- `UNSUPPORTED_SCHEMA_VERSION`
- `CONTRACT_VALIDATION_ERROR`
- `BUSINESS_RULE_VIOLATION`
- `INTEGRITY_CONFLICT`
- `LATE_EVENT`

Word-level invalidity is not quarantine. Invalid words belonging to a processable session stay in `silver/session-words` with `is_valid = false`.

Do not add a Kafka DLQ in the MVP unless explicitly approved.

## Priorities

### P0 - core vertical slice

- Local Compose infrastructure.
- Python simulator.
- Kafka ingestion.
- Spark Structured Streaming.
- Bronze persistence.
- Silver `sessions` and `session-words`.
- Provisional scoring.
- PostgreSQL `serving.provisional_scores`.
- Core unit/integration tests for the vertical slice.

### P1 - daily analytical path

- Daily Spark batch processing.
- Originality metrics.
- Rhyme difficulty metric.
- Final scoring once formula is approved.
- PostgreSQL Gold.
- Daily/weekly/monthly rankings.
- Testing, reproducibility, CI.

### P2 - later improvements

- Quarantine dataset.
- Extended data-quality metrics.
- Metabase dashboards.
- `rebuild_silver_from_bronze` recovery tooling.
- Additional observability beyond essential logs/metrics.

## Acceptance baseline (SMART / SLO / SLI)

These criteria are an agreed initial baseline for implementation and validation. They are deliberately measurable so that the project can demonstrate not only that the architecture works, but how well it works. They are **initial experimental targets**: if measurements later show that an individual threshold is unrealistic or uninformative for the reference workstation, document the evidence and discuss the change with the user. Do not silently relax or replace these targets.

### SMART objectives

1. **SMART-1 - Streaming vertical slice (P0).** Implement a reproducible `simulator -> Kafka -> Spark Structured Streaming -> Bronze + Silver + PostgreSQL provisional` path that correctly processes at least the nominal profile of **1,000 simulated completed sessions**.
2. **SMART-2 - Quality and idempotency (P0).** Demonstrate with automated tests that malformed/invalid input, invalid words, empty valid sessions, and technical retries behave according to the agreed rules, and that one logical event never produces more than one provisional score.
3. **SMART-3 - Daily analytical path (P1).** Implement `Silver -> Spark Batch -> PostgreSQL Gold`, including daily rhyme metrics, originality inputs/metrics, rhyme-difficulty inputs/metrics, final scoring once its formula is approved, and daily/weekly/monthly rankings, with reproducible and idempotent reprocessing.
4. **SMART-4 - Local scale validation (P1).** Validate and document the four agreed data profiles: **20 users/day (demo), 500 (development), 1,000 (nominal), and 60,000 (load test)**, recording latency, throughput, total processing time, and relevant resource observations.
5. **SMART-5 - Reproducible manual demo (before delivery).** Allow the main stages to be invoked explicitly from Bash-compatible scripts/commands so the recorded TFM demonstration can run `message simulation -> streaming results -> forced batch -> Gold` without waiting for the real scheduler or business-day clock.
6. **SMART-6 - Engineering quality (before delivery).** Maintain the solution in Git/GitHub with reproducible local setup instructions, reasonable unit and integration tests for critical behavior, and CI that runs the main automated quality checks.

### Initial SLOs and SLIs

| Concern | SLI / measurement | Initial SLO | Applies to |
|---|---|---|---|
| Provisional scoring latency | `serving.provisional_scores.calculated_at - Kafka timestamp` | **p95 <= 5 s; p99 <= 10 s** | Nominal profile |
| Bronze ingestion completeness | Physical Kafka deliveries successfully processed vs. Bronze records | **100% of processed Kafka deliveries are represented in Bronze** | Functional/integration tests |
| Provisional idempotency | Number of provisional-score rows per accepted logical session/event | **Exactly 1** | Duplicate-delivery tests |
| Accelerated-load throughput | Fully processed session events / elapsed seconds | **>= 100 events/s average** | 60,000-user load profile |
| Accelerated-load completion | Elapsed time to process the 60,000-session test workload | **<= 10 min** | 60,000-user load profile |
| Daily batch runtime | Batch start to successful Gold replacement for the target scope | **<= 10 min** | 60,000-user load profile |
| Gold reprocessing idempotency | Functional comparison after rerunning the same business-date scope | **Same functional result; 0 duplicate logical rows** | Batch integration tests |
| Recorded-demo flow | Start of 20-user simulation to inspectable forced-batch Gold result, with infrastructure already running | **<= 3 min** | Demo profile |

Do not require the nominal streaming-latency SLO during the intentionally accelerated 60,000-session load test. The load test is primarily a throughput, integrity, and total-runtime exercise.

The detailed acceptance matrix, measurement method, commands used, measured results, and PASS/FAIL evidence must live in a repository document created with the scaffolding:

```text
docs/acceptance-criteria.md
```

`AGENTS.md` keeps the binding baseline; `docs/acceptance-criteria.md` keeps the operational detail and experimental results; the TFM memory should summarize the SMART objectives, validation methodology, and principal measured outcomes.

## Quality and TFM constraints

The implementation must support the academic deliverables and tutor expectations:

- Git/GitHub version control.
- Reasonable unit-test coverage.
- Integration testing for important pipeline behavior.
- Reproducible local setup and README instructions.
- CI should be added as part of the quality phase.
- Code quality and readability are important evaluation criteria.
- Architecture decisions must be explainable and justifiable in the TFM memory.
- Generated/simulated data only; do not introduce real personal data without explicit scope change.

At minimum, plan tests for:

- normalization
- dictionary validation
- rhyme validation
- duplicate words in one session
- empty session
- provisional scoring
- Kafka technical retry/idempotency
- Bronze retaining physical duplicates
- Silver tolerating structured technical duplicates across microbatches
- PostgreSQL provisional-score idempotency under duplicate delivery
- Batch deduplication by `event_id` before Gold calculations
- First-accepted-event-wins behavior for multiple distinct `event_id` values sharing the same `user_id + business_date`
- Gold reprocessing/idempotency when implemented

## Current open decisions

Do not silently resolve the following items. Discuss them with the user when they become relevant:

1. Exact final scoring formula and weights.
2. Exact small post-cutoff readiness delay/check for the cron-triggered batch.
3. Default simulator word-count mean/standard deviation, event size, and exact simulator error-injection percentages. User-count profiles are agreed as 20 demo / 500 development / 1,000 nominal / 60,000 load test; mean and standard deviation are configurable parameters and final defaults are still pending.
4. Monitoring/observability implementation.
5. Metabase dashboards.
6. Final availability and inspection of `basque_words.txt`.
7. Whether any graph-based word/rhyme analysis remains in scope.
8. Project calendar and milestone dates.

## Resume point

At this checkpoint, the Kafka transport and v1 input contract, the main streaming/batch data flow, the Medallion responsibilities, the lexical validation concept, the conceptual originality/rhyme-difficulty metrics, the cumulative/average weekly-monthly ranking model, and the business-day cutoff policy are agreed. Manual execution of message simulation, Spark streaming, and Spark batch (including an explicit local/demo force path) is also required and now has fixed Bash wrapper names. Daily batch orchestration is agreed as a lightweight cron-based scheduler service in Compose, not Airflow/Prefect/Dagster; the scheduler reuses the Spark batch-capable runtime and the exact same batch implementation without access to the container-engine socket. Spark watermarking is deliberately excluded from the MVP; persistent MinIO-backed checkpoints remain required, streaming deduplication is best-effort only, PostgreSQL protects provisional-score idempotency, and the daily batch performs the definitive deduplication before Gold. The Python simulator is Bash-invokable, deterministic when seeded, configurable for empty sessions, positive word-count generation via mean/standard-deviation parameters, and two independent invalid-word injection modes, and sized for 20/500/1,000/60,000-user demo/development/nominal/load profiles. Python 3.10 is fixed, local dependency isolation uses standard `venv`, and the container-first design must keep custom service images lean. Dependency files are split into `requirements/simulator.txt`, `requirements/spark.txt`, and development-only `requirements/dev.txt`, with each runtime image installing only its own dependency set. The repository scaffolding is now fixed as a monorepo with `services/`, `infra/`, `scripts/`, `contracts/`, `data/`, `tests/`, and `docs/`; Compose service names and explicit Kafka/MinIO/PostgreSQL bootstrap responsibilities are also fixed. Streaming and batch share one project-owned Spark codebase/runtime image while remaining separate execution modes. Silver schemas are agreed, including `source_record_id`, SHA-256 `payload_hash`, word-level `word_index`, and the rule that only the first occurrence of a normalized word may be valid within a session. PostgreSQL schemas, keys, indexes, streaming insert/idempotency rules, the **first accepted event wins** user/day conflict policy, materialized daily/weekly/monthly rankings, and transactional Gold replace-by-scope reprocessing are agreed. `gold.business_kpis` remains intentionally deferred until a concrete Metabase need exists. The initial SMART/SLO baseline is also agreed: nominal scoring latency p95 <= 5 s / p99 <= 10 s, accelerated-load throughput >= 100 events/s, 60,000-session load and batch targets <= 10 min, deterministic idempotency criteria, and a <= 3 min 20-user demo flow with infrastructure already running. The scaffolding must include `docs/acceptance-criteria.md` for the detailed measurement plan and experimental evidence. Continue with the remaining open design decisions; do not revisit agreed items unless the user asks to change them.
