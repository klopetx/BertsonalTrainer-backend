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
- Cron: lightweight scheduling for the single daily batch job.

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

Conceptual Bash usage should remain simple, for example a wrapper or command that ultimately invokes the Python simulator with parameters for user count, business date/rhyme scenario, seed, delay, word-count mean/standard deviation, empty-session rate, and error-injection rates. Exact script/module names are deferred until repository scaffolding is agreed.

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
    |-- rhyme_daily_metrics
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
- `gold.weekly_rankings` and `gold.monthly_rankings` must store both cumulative and average performance measures.
- At minimum, preserve `total_score`, `average_score`, and `days_played` for weekly/monthly periods.
- `gold.weekly_rankings` and `gold.monthly_rankings` must materialize both `rank_by_total` and `rank_by_average`.
- `gold.rhyme_daily_metrics` is approved to store day/rhyme aggregates such as participant count, total valid words, average valid words per session, and the derived rhyme-difficulty metric.

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
- The exact small technical delay/readiness check between the `01:00` cutoff and automatic batch start remains to be defined together with orchestration/checkpoint semantics.

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

The batch is scheduled by a lightweight cron-based scheduler service in Compose. The business-day acceptance cutoff is agreed; Spark watermark/checkpoint and the small post-cutoff readiness delay are still pending.

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

The exact CLI/script names are intentionally deferred until the repository scaffolding and Python packaging/tooling are agreed.

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
4. Spark checkpoint/watermark and post-cutoff batch-readiness details.
5. Exact small post-cutoff readiness delay/check for the cron-triggered batch.
6. Default simulator word-count mean/standard deviation, event size, throughput targets, and exact simulator error-injection percentages. User-count profiles are agreed as 20 demo / 500 development / 1,000 nominal / 60,000 load test; mean and standard deviation are configurable parameters and final defaults are still pending.
7. Concrete SMART/SLO acceptance targets for streaming latency and batch runtime.
8. Exact Python version and dependency/package manager.
9. Detailed repository scaffolding/module layout and exact manual/demo CLI commands.
10. Monitoring/observability implementation.
11. Metabase dashboards.
12. Final availability and inspection of `basque_words.txt`.
13. Whether any graph-based word/rhyme analysis remains in scope.
14. Project calendar and milestone dates.

## Resume point

At this checkpoint, the Kafka transport and v1 input contract, the main streaming/batch data flow, the Medallion responsibilities, the lexical validation concept, the conceptual originality/rhyme-difficulty metrics, the cumulative/average weekly-monthly ranking model, and the business-day cutoff policy are agreed. Manual execution of message simulation, Spark streaming, and Spark batch (including an explicit local/demo force path) is also a required capability. Daily batch orchestration is agreed as a lightweight cron-based scheduler service in Compose, not Airflow/Prefect/Dagster. The Python simulator is required to be Bash-invokable, deterministic when seeded, configurable for empty sessions, positive word-count generation via mean/standard-deviation parameters, and two independent invalid-word injection modes, and sized for 20/500/1,000/60,000-user demo/development/nominal/load profiles. Continue with the remaining open design decisions; do not revisit agreed items unless the user asks to change them.
