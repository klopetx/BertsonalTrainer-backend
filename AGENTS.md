# BertsonalTrainer-backend

## Purpose
- Backend/data platform for the BertsonalTrainer Master's Thesis (Big Data & Data Engineering).
- Demonstrates a synthetic end-to-end flow: simulator → Kafka → Spark Structured Streaming → MinIO Bronze/Silver → PostgreSQL (serving + Gold) → later Metabase.
- Uses only generated data; never ingest real personal data without explicit scope change.

## Architecture Overview
```
Python simulator -> Kafka -> Spark Structured Streaming
      |                     |---> MinIO Bronze (immutable raw)
      |                     |---> MinIO Silver (sessions + session-words)
      |                     `---> PostgreSQL serving.provisional_scores
Silver -> daily Spark batch -> PostgreSQL gold.* tables -> (future) Metabase
```
- Bronze remains the long-term audit log; Silver is structured but may contain technical duplicates.
- Daily Spark batch enforces definitive deduplication, originality inputs, and applies hardness (difficulty) weighting before writing Gold tables.

## Core Invariants (do not break)
- Python 3.10 only; local envs use `python -m venv` and no alternate package managers.
- Requirements are split (`requirements/simulator.txt`, `requirements/spark.txt`, `requirements/dev.txt`).
- Container-first workflow on Podman/`compose.yaml`; no Docker Desktop-only features.
- Runtime images stay lean; install only service-specific dependencies; infrastructure services use upstream images.
- No REST/API surface in the MVP; focus stays on data engineering goals.
- No Airflow/Prefect/Dagster; cron scheduler (inside its own service) reuses the Spark batch image.
- One Kafka message = one completed session; at most one official functional session per user+business day.
- **First accepted event wins** for conflicting `event_id`s affecting the same user/day; Kafka single partition + `user_id` key preserve ordering.
- Bronze is immutable; Silver is structured but not the final idempotency boundary; PostgreSQL protects provisional-score idempotency (`ON CONFLICT DO NOTHING`).
- Daily batch performs definitive deduplication before Gold and replaces scopes transactionally.
- No Spark watermarking in the MVP; persistent checkpoints in MinIO (`system/checkpoints/streaming/<query-name>/`) are mandatory.
- The same normalization/validation logic must be reused everywhere; never pre-normalize simulator output.
- Do not invent new scoring weights/formulas beyond the currently documented model, or simulator default parameters, without user approval.
- Never make material architecture/scope decisions silently; escalate open decisions when relevant.

## Priorities
- **P0:** Local Compose infra, simulator, Kafka ingestion, Structured Streaming, Bronze/Silver, provisional scoring, serving table, core tests.
- **P1:** Daily Spark batch, originality + hardness weighting, PostgreSQL Gold, daily/weekly/monthly rankings, testing/CI.
- **P2:** Quarantine dataset, extended data-quality/observability, Metabase dashboards, `rebuild_silver_from_bronze`, other nice-to-haves.

## Repository & Runtime Conventions
- Monorepo with `services/`, `infra/`, `scripts/`, `tests/`, `docs/`, `contracts/`, and `data/reference/` (see `docs/architecture.md`).
- `services/spark` houses one shared codebase/runtime for streaming, batch, and scheduler execution.
- Official human-facing commands live under `scripts/` (listed below); new tooling must plug into this interface.
- Container-first execution: run services through Compose, not via host Python interpreters.
- Reference environment: Windows workstation, Podman, `podman compose up -d`, ≥16 GB RAM, 20–25 GB free disk, local-first MVP, no cloud deployment.

## Manual/Demo Commands
- `./scripts/infra-up.sh` — start Kafka/MinIO/PostgreSQL + helpers.
- `./scripts/streaming-up.sh` — launch Structured Streaming with checkpoints.
- `./scripts/simulate.sh ...` — run the Python simulator (forwards CLI parameters).
- `./scripts/batch-run.sh --business-date YYYY-MM-DD [--force]` — run the daily batch; `--force` is for local/test/demo only.
- `./scripts/scheduler-up.sh` — start cron-based automation (same batch implementation).
- `./scripts/logs.sh <service>` — inspect service logs.
- `./scripts/test.sh` — execute tests.
- `./scripts/down.sh` — stop services without deleting persistent volumes.

## Open Decisions (do not resolve silently)
1. Scheduler post-cutoff readiness delay/check.
2. Exact post-cutoff readiness delay/check before the scheduler launches the batch.
3. Default simulator word-count mean/stddev, event size profile, and exact error-injection percentages (empty sessions, random words, typos).
4. Monitoring/observability tooling.
5. Metabase dashboard scope/timing.
6. Final availability/inspection plan for `data/reference/ordered_basque_dictionary.csv`.
7. Whether any graph-based word/rhyme analysis remains in scope.
8. Project calendar and milestone targets.

## Documentation Map (read on demand)
- Architecture, runtime, repo layout, scripts, checkpoints → `docs/architecture.md`.
- Kafka topology, `session-events` contract, technical retries, late events → `docs/kafka-contract.md`.
- Bronze/Silver schemas, normalization/validation flow, checkpoint namespace, quarantine plan → `docs/medallion-model.md`.
- PostgreSQL serving + Gold schemas, constraints, replace-by-scope strategy → `docs/postgres-model.md`.
- Business-day cutoff, batch responsibilities, originality + hardness weighting, rankings, cron rules → `docs/batch-and-scoring.md`.
- Simulator behavior, workload profiles, CLI parameters, dictionary references → `docs/simulator.md`.
- SMART objectives, SLO/SLI targets, measurement evidence → `docs/acceptance-criteria.md`.
- Key rationale and historical decisions → `docs/decisions.md`.

## Lazy-loading instruction
Do **not** preload every design document. Read only the file(s) relevant to the current task, and reference them before modifying an established contract, schema, business rule, or architectural component. When a task touches multiple areas (for example, Kafka contract plus Silver schema), load the corresponding documents explicitly.
