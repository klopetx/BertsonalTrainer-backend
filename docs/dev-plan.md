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

## Phase 2 — Core Infra + Streaming Ingestion (P0 remainder)
**Status:** ✅ streaming path completed (infra + ingestion + provisional serving)
**Goal:** End-to-end ingestion from Kafka into Bronze/Silver + provisional PostgreSQL.

### Deliverables
1. [x] `kafka-init` helper service to declare `session-events` (1 partition, 3-day retention) automatically. (See `compose.yaml` service `kafka-init`.)
2. [x] MinIO + PostgreSQL services in Compose with init containers:
   - `minio`: buckets for Bronze (`bronze/session-events/…`), Silver, checkpoints (`system/checkpoints/streaming/<query>/`).
   - `postgres`: bootstrap schema per `docs/postgres-model.md` for `serving.provisional_scores`.
3. [x] Spark streaming service (`services/spark`):
   - Docker image installing Spark deps + shared project code.
   - Production ingestion entrypoint (`bertsonal_spark/streaming/main.py`) wiring Kafka → Bronze (raw parquet + lineage), Silver sessions/words (contract validation, duplicate handling, late-event filtering), and `serving.provisional_scores` inserts.
   - Persistent checkpoints volume in MinIO under `system/checkpoints/streaming/<query>/`.
4. [x] Scripts: `./scripts/infra-up.sh`, `./scripts/streaming-up.sh`, `./scripts/down.sh`, `./scripts/logs.sh <service>` keep podman usage consistent across environments.

### Verification
- Integration test: `infra-up` → `streaming-up` → simulator → observe Bronze parquet, Silver parquet (partitioned by `business_date`), and provisional-score inserts; repeatable via `scripts/test.sh` streaming suite.
- Bronze partitions capture raw Kafka payload + metadata; Silver tables reflect validation + duplicate rules; PostgreSQL provisional table enforces `ON CONFLICT DO NOTHING` for idempotency.
- Logs surface late/invalid event drops (e.g., `CONTRACT_VALIDATION_ERROR`, `LATE_EVENT`) instead of silently discarding data.

## Phase 3 — Daily Batch + Scheduler (P1)
**Status:** 🟡 skeletal batch/scheduler landed; scoring/rankings TBD
**Goal:** Deterministic Gold tables and cron automation.

### Deliverables
1. [🟡] Spark batch job (`services/spark/batch`) now ingests Silver partitions, applies first-event-wins deduplication, and populates `gold.daily_scores` + `gold.rhyme_daily_metrics`. Rankings remain TODO.
2. [🟡] Scheduler service (`services/spark/scheduler`) reuses the Spark image and polls post-cutoff; readiness delay rules still open.
3. [x] Script wrappers: `./scripts/batch-run.sh --business-date YYYY-MM-DD [--force]` and `./scripts/scheduler-up.sh` enable manual and continuous runs.
4. [🟡] Tests: basic cutoff/unit coverage lands in `tests/batch/`; end-to-end replace-by-scope verification pending future scoring work.

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
1. Add the remaining orchestration scripts (`down`, `logs`) and shared stopping workflow.
2. Extend tests (unit + integration) as components land, ensuring `scripts/test.sh` remains the single entry point.
3. Prepare Spark batch scaffolding + scheduler wiring (Phase 3) while open design decisions (final scoring weights, readiness delay) remain outstanding.
