# BertsonalTrainer-backend

Author: Kerman Lopez de Calle

## Quick Start

1. **Install prerequisites**: Podman (or Docker Desktop with podman compatibility) and Python 3.11 for local tests.
2. **Start infra + streaming**
   ```bash
   ./scripts/streaming-up.sh
   ```
3. **Publish sample sessions**
   ```bash
   ./scripts/simulate.sh --users 2 --business-date 2026-09-03 --rhyme ari --rhyme-id R001 --seed 42
   ```
4. **Inspect outputs**
   - Bronze parquet: `s3a://bronze/session-events/`
   - Silver parquet: `s3a://silver/sessions/` and `s3a://silver/session-words/`
   - Provisional scores: `podman compose exec postgres psql -U bertsonal -d bertsonal -c "select * from serving.provisional_scores;"`
5. **Run the daily batch (after the cutoff or with `--force`)**
   ```bash
   ./scripts/batch-run.sh --business-date 2026-09-03 --force
   ```
6. **Optional scheduler demo**
   ```bash
   ./scripts/scheduler-up.sh
   ```

## Streaming Highlights

- **Contract validation** enforced per `docs/kafka-contract.md` (schema version, UUIDs, timestamps, word arrays).
- **Late-event handling**: events arriving after the `Europe/Madrid 01:00` grace window land in Bronze but are skipped for Silver/Postgres; logs emit `LATE_EVENT` counters.
- **Normalization + validation**: shared logic in `bertsonal_spark/common/validation.py` powers both streaming and future batch jobs.
- **Checkpoints & MinIO**: each Structured Streaming query writes under `s3a://system/checkpoints/streaming/<query>/` for deterministic restarts.
- **Daily batch**: `spark-batch` service reads Silver partitions, applies first-event-wins deduplication, and writes preliminary `gold.daily_scores` + `gold.rhyme_daily_metrics`. Use `batch-run.sh` for manual execution or `scheduler-up.sh` for continuous polling after the `01:00 Europe/Madrid` cutoff.

Refer to `docs/dev-plan.md` for the phase-by-phase roadmap and remaining work (batch job, scheduler, observability tooling).

## Acceptance Evidence

Acceptance targets and evidence capture live in `docs/acceptance-criteria.md`.

- Run the acceptance scripts locally to generate evidence artifacts.
- Commit small summaries under `docs/evidence/` and keep full raw logs under `evidence/runs/` (gitignored).
