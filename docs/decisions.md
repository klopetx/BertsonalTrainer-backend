# Architectural Decision Highlights

This log summarizes the most important decisions recorded in `AGENTS.md`, along with their rationale. Consult the linked design documents for implementation details.

## Architecture and runtime

- **Kafka as transport, Bronze as history.** Kafka retains only a short buffer (3 days). Persisting every delivery in immutable Bronze ensures long-term auditability and enables future `rebuild_silver_from_bronze` tooling without depending on Kafka retention.
- **Bronze is immutable.** Append-only storage keeps the ingestion trail trustworthy for audits, troubleshooting, and deterministic reprocessing.
- **Silver is structured but not final deduplication.** Streaming may observe technical retries; the batch performs definitive deduplication so correctness does not rely on microbatch-level heuristics.
- **One shared Spark codebase/runtime.** Streaming and batch share `bertsonal_spark` to prevent divergence in normalization, validation, and scoring logic.
- **Cron instead of Airflow/Prefect/Dagster.** The MVP orchestrates a single daily batch. A lightweight cron container is easier to operate on the reference workstation and avoids unnecessary platform overhead.
- **No Spark watermarking in the MVP.** The workload does not require windowing or stream-stream joins. Explicit business-day cutoffs plus checkpointing meet the requirements with less stateful complexity.
- **Container-first execution with lean images.** Compose keeps services isolated, reproduces the local environment, and avoids cross-contamination of dependencies. Splitting `requirements/` by service prevents installing unnecessary packages into every image.
- **Python 3.10 + `venv`.** Fixing the runtime version and using the standard library’s virtual environments keeps the stack reproducible without introducing additional tooling such as Poetry or Conda.
- **No REST API in the MVP.** The project focuses on data engineering deliverables; adding product-facing APIs would expand scope without supporting the current thesis goals.

## Data integrity and scoring

- **One Kafka message per completed session.** Simplifies ingestion reasoning and aligns simulator output with downstream idempotency guarantees.
- **First accepted event wins.** Deterministic ordering (single Kafka partition keyed by `user_id`) allows the system to ignore conflicting later events while preserving technical retries.
- **Separate provisional and final scores.** Streaming writes provisional results quickly, while the batch recomputes final scores with originality and rhyme-difficulty context. Keeping them separate preserves auditability.
- **Gold replace-by-scope writes.** Transactions delete and re-insert each affected scope (day/week/month) so reprocessing cannot leave stale rows or duplicate rankings.
- **Business-day cutoff with 1-hour grace.** Aligns with the product’s Europe/Madrid context and keeps late-arriving sessions predictable. Late events stay in Bronze but may be classified as `LATE_EVENT` elsewhere.
- **Consistent normalization/validation rules.** Streaming and batch must share the same logic so provisional and final scoring stay aligned.
- **Quarantine deferred but approved.** Bad events route to quarantine instead of crashing streaming once that area is implemented, keeping Bronze intact while isolating problematic input.
- **`business_kpis` deferred.** Gold tables already cover the metrics needed for the MVP. Additional KPI tables will be reconsidered when Metabase requirements appear.
