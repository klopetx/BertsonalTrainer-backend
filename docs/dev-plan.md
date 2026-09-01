# Development Plan

This living document breaks down the BertsonalTrainer backend build into sequenced phases that align with the priorities and invariants defined in `AGENTS.md`. Each phase highlights core deliverables, dependencies, verification steps, and the reference docs to consult before implementing.

## Principles
- **Contract-first** — reference `docs/kafka-contract.md`, `docs/medallion-model.md`, and `docs/postgres-model.md` before touching payloads or schemas.
- **Container-first** — every service starts via `compose.yaml` and `scripts/` wrappers; never rely on host-only execution.
- **Deterministic + Tested** — small, test-backed commits (`scripts/test.sh` for unit/integration suites) with reproducible seeds in simulator workloads.
- **Observability-ready** — structured logging and auditability (Bronze immutability, topic metrics) wired in from the start to avoid retrofits.
- **Decision escalation** — open items listed in `AGENTS.md` stay unresolved until explicitly approved; do not silently pick defaults.

## Phase 1 — Simulator + Kafka (P0 subset)
**Status:** ✅ completed

- Simulator container + CLI (`services/simulator`) respecting configurable parameters and Basque dictionary mounting (`data/reference`).
- Kafka single-node broker in Compose with manual topic creation.
- Smoke tests + unit tests (`tests/simulator/`) proving payload generation matches `docs/kafka-contract.md`.

## Phase 2 — Core Infra + Streaming Skeleton (P0 remainder)
**Goal:** End-to-end ingestion from Kafka into Bronze/Silver + provisional PostgreSQL.

### Deliverables
1. [x] `kafka-init` helper service to declare `session-events` (1 partition, 3-day retention) automatically. (See `compose.yaml` service `kafka-init`.)
2. [x] MinIO + PostgreSQL services in Compose with init containers:
   - `minio`: buckets for Bronze (`bronze/session-events/…`), Silver, checkpoints (`system/checkpoints/streaming/<query>/`).
   - `postgres`: bootstrap schema per `docs/postgres-model.md` for `serving.provisional_scores`.
3. [ ] Spark streaming service scaffold (`services/spark`):
   - Shared image with Structured Streaming job reading Kafka → Bronze (immutable), Silver (structured sessions), provisional scores.
   - Persistent checkpoints in MinIO.
   - Normalization/validation shared module per `docs/medallion-model.md`.
4. [ ] Scripts: `./scripts/infra-up.sh` (✅ implemented), plus upcoming `./scripts/streaming-up.sh`, `./scripts/down.sh`, `./scripts/logs.sh <service>`.

### Verification
- Integration test: simulator → Kafka → Spark streaming → MinIO/ Postgres (can be manual initially, later automated via `scripts/test.sh` profile).
- Bronze partitions contain raw Kafka payload + metadata; Silver tables reflect dedup rules; PostgreSQL provisional table shows inserts with `ON CONFLICT DO NOTHING` semantics.

## Phase 3 — Daily Batch + Scheduler (P1)
**Goal:** Deterministic Gold tables and cron automation.

### Deliverables
1. Spark batch job (`services/spark/batch`) implementing:
   - Bronze → Silver replay (dedupe), originality + rhyme-difficulty metrics (pending open decisions).
   - Gold tables + rankings per `docs/batch-and-scoring.md` and `docs/postgres-model.md`.
2. Scheduler service reusing Spark batch image (`services/spark/scheduler` or similar) to run cron after business-day cutoff (`Europe/Madrid 01:00` + readiness delay once defined).
3. Script wrappers: `./scripts/batch-run.sh --business-date YYYY-MM-DD [--force]` and `./scripts/scheduler-up.sh`.
4. Tests: unit tests for scoring logic, integration tests for replace-by-scope writes.

### Verification
- Batch rerun idempotency (same date yields same Gold state).
- Scheduler logs confirm adherence to cutoff + readiness delay.
- Metrics validated against acceptance criteria in `docs/acceptance-criteria.md`.

## Phase 4 — Enhancements & P2 Items
**Goal:** Improve resiliency, observability, and analytic surfaces.

- Quarantine dataset + replay tooling (`docs/medallion-model.md`).
- Observability: metrics exporter, structured logs aggregated via `scripts/logs.sh`.
- Metabase (or alternative) pointing at PostgreSQL Gold tables.
- `rebuild_silver_from_bronze` utility for backfills.
- Final Basque dictionary availability plan (per open decision #6) once defined.

## Working Practices
1. **Trunk-based dev** with short-lived branches; commit scope stays tight (tests + code together).
2. **Scripts before docs drift** — every manual command gets a wrapper in `scripts/` to keep demos reproducible.
3. **Cross-service test matrix** — expand `scripts/test.sh` to accept markers (e.g., `--with-streaming`) for heavier suites.
4. **Code reviews** focus on contract adherence (reference relevant doc section in PR description).
5. **Documentation hygiene** — update this plan and affected docs when scope changes; link commits to plan sections for traceability.

## Next Actions
1. Scaffold the Spark streaming container and codebase per Phase 2 goals.
2. Add the remaining orchestration scripts (`streaming-up`, `down`, `logs`) once streaming is ready.
3. Extend tests (unit + integration) as components land, ensuring `scripts/test.sh` remains the single entry point.
