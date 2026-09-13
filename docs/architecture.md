# Architecture and Runtime Design

This document captures the agreed reference environment, repository layout, container strategy, and execution interfaces for the BertsonalTrainer backend. It explains how the streaming and batch components coexist in one monorepo and why the project relies on a lightweight cron-based scheduler instead of a heavier orchestrator.

## Reference environment

- Host OS: Windows workstation with at least 16 GB RAM.
- Container engine: Podman. Do not depend on Docker Desktop-only behavior.
- Local orchestration: `compose.yaml` with the reference command `podman compose up -d`.
- Recommended free disk before running the stack: 20-25 GB.
- The MVP is local-first; cloud deployment is explicitly out of scope.

## Streaming + batch rationale

The system follows a lambda-style flow: a Python simulator publishes completed-session events to Kafka, Spark Structured Streaming ingests them into MinIO Bronze/Silver and PostgreSQL provisional serving, and a daily Spark batch job derives Gold tables after the business-day close. Keeping both streaming and batch inside one repository ensures that validation, normalization, and scoring rules remain consistent across execution modes. Metabase will consume PostgreSQL later and is lower priority.

## Python runtime and container constraints

- All custom Python code (simulator, Spark helpers, tooling) targets Python **3.11** only.
- Local development environments use `python -m venv`; never switch package managers without explicit approval.
- The application is container-first: services run inside Compose rather than on the host Python install.
- Keep runtime images lean. Prefer `python:3.11-slim` (or similarly slim bases) for Python-only services and rely on official upstream images for Kafka, MinIO, PostgreSQL, and Spark.
- Install only the dependencies required by each service. Dependency files are split by concern:

```text
requirements/
|-- simulator.txt
|-- spark.txt
`-- dev.txt
```

- Do not install `requirements/dev.txt` in production images.
- Multi-stage builds are allowed when native build tooling would otherwise bloat the runtime image.
- Compose is the integration boundary: services communicate via declared network endpoints and persistent volumes.

## Repository scaffolding

The monorepo structure is fixed:

```text
BertsonalTrainer-backend/
|-- AGENTS.md
|-- README.md
|-- compose.yaml
|-- .env.example
|-- requirements/
|-- contracts/
|-- data/reference/
|-- services/
|   |-- simulator/
|   `-- spark/
|-- infra/
|   |-- kafka/
|   |-- minio/
|   |-- postgres/
|   `-- scheduler/
|-- scripts/
|-- tests/
`-- docs/
```

- `services/` hosts project-owned application code. The Spark directory contains `bertsonal_spark` with `common`, `streaming`, and `batch` packages that share one runtime image.
- `infra/` stores bootstrap assets for Kafka, MinIO, PostgreSQL, and the scheduler.
- `scripts/` is the supported Bash-compatible interface exposed to teammates and during the recorded demo.
- `tests/` separates unit, integration, and performance suites plus deterministic fixtures.
- `docs/` contains detailed design material referenced from `AGENTS.md`.

## Compose services

Approved service names:

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

- `kafka-init` must explicitly create the `session-events` topic (1 partition, replication factor 1, 3-day retention). Do not rely on auto-creation.
- `minio-init` prepares the required buckets/namespaces for Bronze, Silver, checkpoints, and future quarantine storage.
- `spark-streaming` runs the Structured Streaming job that ingests Kafka events into Bronze/Silver and PostgreSQL serving.
- `spark-batch` runs the one-shot daily batch entrypoint.
- `scheduler` reuses the Spark batch-capable image and executes cron plus `spark-submit`. It is not granted access to the container-engine socket; all work happens inside the service itself.

Metabase may be added later (P2).

## Execution scripts and demo interface

The human-facing commands are fixed and must wrap the real containerized implementation:

- `./scripts/infra-up.sh` — start Kafka, MinIO, PostgreSQL, and supporting services.
- `./scripts/streaming-up.sh` — launch Spark Structured Streaming with persistent checkpoints.
- `./scripts/simulate.sh` — invoke the Python simulator CLI with passthrough parameters (users, business date, rhyme, seed, delay, mean/stddev, empty-session rate, error-injection rates).
- `./scripts/batch-run.sh --business-date YYYY-MM-DD [--force]` — run the daily batch manually, with `--force` reserved for development/demo before the real cutoff.
- `./scripts/scheduler-up.sh` — start the cron-based scheduler service.
- `./scripts/logs.sh <service>` — inspect logs for a specific service.
- `./scripts/test.sh` — execute project tests.
- `./scripts/down.sh` — stop the environment without deleting persistent volumes.

Manual demo flow (infrastructure already running):

```text
start streaming -> run simulator -> inspect Bronze/Silver/serving
-> (optional) force batch for the chosen business date -> inspect Gold/rankings
```

## Scheduler architecture

- Cron runs inside the dedicated `scheduler` service.
- The scheduler invokes the exact same `bertsonal_spark/batch/main.py` entrypoint used by the manual path; there is no alternate batch implementation.
- Normal automatic execution occurs after the previous business day's `01:00 Europe/Madrid` cutoff. A small readiness delay/check is still pending and must be agreed before automation depends on it.
- A `--force` capability exists solely for local development/tests/demos and must remain explicit.
- Re-running the scheduler or manual batch for the same business date must be idempotent thanks to Gold's replace-by-scope writes.

## Streaming checkpoints

- Spark Structured Streaming stores checkpoints in MinIO under `system/checkpoints/streaming/<query-name>/`.
- Each query must use its own checkpoint path.
- Persistent checkpoints are mandatory so the query can resume progress after restarts.
- No Spark watermarking is required for the MVP; late-event handling is enforced as an explicit business rule described in `docs/batch-and-scoring.md`.

## Container responsibilities

- The simulator image should remain Python-only (conceptually `python:3.11-slim + requirements/simulator.txt + simulator code`).
- Spark streaming, batch, and scheduler reuse one project-owned Spark runtime image.
- Infrastructure components (Kafka, MinIO, PostgreSQL) rely on official upstream images.
- Avoid unnecessary OS packages, cached artifacts, or compiler toolchains in runtime images. Use multi-stage builds if native extensions are required.

## Rationale

- One shared Spark codebase prevents drift between streaming and batch logic.
- Keeping Bronze immutable provides an auditable raw history, enabling future `rebuild_silver_from_bronze` tooling without revisiting Kafka.
- Cron is sufficient because the workflow graph is a single daily batch with simple dependencies; heavier orchestrators would add operational overhead with no proportional benefit on the reference workstation.
