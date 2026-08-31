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

- Python: simulator and project tooling.
- Kafka: event broker.
- Apache Spark Structured Streaming: near-real-time ingestion, validation, normalization, Silver generation, and provisional scoring.
- Apache Spark batch: daily final scoring and aggregations.
- MinIO: Bronze and Silver data lake storage.
- PostgreSQL: serving and Gold relational storage.
- Metabase: final BI layer, lower priority.
- Compose: local service orchestration.

Do not add an HTTP/REST API to the MVP unless explicitly approved later.

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

- One row per logical deduplicated session.
- A user has at most one logical session per `business_date`.
- Empty sessions are retained.
- Session-level metrics may include submitted/valid word counts and processing metadata.

### `silver/session-words`

- One row per submitted word.
- Keep both valid and invalid words.
- Retain the original and normalized word forms.
- Include `is_valid`.
- Include `validation_reason` when invalid.
- Link every row to its `session_id`.
- Only `is_valid = true` rows contribute to scoring/originality/rankings.

Technical retry duplicates are preserved in Bronze but must resolve to one logical session in Silver.

## PostgreSQL

Use one PostgreSQL instance but separate provisional serving data from final Gold data using schemas.

```text
PostgreSQL
|-- serving
|   `-- provisional_scores
`-- gold
    |-- daily_scores
    |-- weekly_rankings
    |-- monthly_rankings
    `-- business_kpis
```

Rules:

- Do not overwrite provisional scores with final scores.
- Provisional and final scores must remain independently auditable.
- `serving.provisional_scores` must be written idempotently.
- Final Gold loads must also be idempotent/re-runnable.
- Exact keys, indexes, and upsert strategy are still pending design.

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

## Batch processing

The normal daily batch consumes Silver, not Kafka and not Bronze.

Responsibilities include:

- daily word-frequency aggregates
- originality metrics
- rhyme difficulty metrics
- final daily score
- daily ranking
- weekly ranking
- monthly ranking
- Gold KPI generation

The exact orchestration tool and daily close/watermark policy remain pending.

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
- Silver producing one logical session
- Gold reprocessing/idempotency when implemented

## Current open decisions

Do not silently resolve the following items. Discuss them with the user when they become relevant:

1. Exact final scoring formula and weights.
2. Exact `silver/sessions` and `silver/session-words` schemas.
3. PostgreSQL keys, indexes, and upsert implementation.
4. Logical day-close policy and late-event handling.
5. Spark checkpoint/watermark details.
6. Batch orchestration tool and schedule.
7. Expected number of simulated users, average words/session, event size, and throughput targets.
8. Concrete SMART/SLO acceptance targets for streaming latency and batch runtime.
9. Exact Python version and dependency/package manager.
10. Detailed repository scaffolding/module layout.
11. Monitoring/observability implementation.
12. Metabase dashboards.
13. Final availability and inspection of `basque_words.txt`.
14. Whether any graph-based word/rhyme analysis remains in scope.
15. Project calendar and milestone dates.

## Resume point

At this checkpoint, the next design topic should be selected from the open decisions above. The Kafka transport and v1 input contract, the main streaming/batch data flow, the Medallion responsibilities, the lexical validation concept, and the conceptual originality/rhyme-difficulty metrics are already agreed.
