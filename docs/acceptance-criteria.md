# Acceptance Criteria, SMART Objectives, and SLO Baseline

These targets define how the BertsonalTrainer backend demonstrates functional completeness, quality, and reproducibility. They are **initial experimental baselines**. If real measurements show a specific threshold is unrealistic or uninformative for the reference workstation, document the evidence and discuss any change with the user—do not silently relax or replace the targets.

## SMART objectives

1. **SMART-1 — Streaming vertical slice (P0).** Deliver a reproducible `simulator -> Kafka -> Spark Structured Streaming -> Bronze + Silver + PostgreSQL provisional` path that processes at least **1,000 simulated completed sessions**.
2. **SMART-2 — Quality and idempotency (P0).** Provide automated tests proving that malformed/invalid input, invalid words, empty sessions, and technical retries behave according to the agreed rules, and that one logical event never produces more than one provisional score.
3. **SMART-3 — Daily analytical path (P1).** Implement `Silver -> Spark Batch -> PostgreSQL Gold`, including daily rhyme metrics, originality metrics, hardness (difficulty) weighting, and daily/weekly/monthly rankings with reproducible, idempotent reprocessing.
4. **SMART-4 — Local scale validation (P1).** Validate and document four profiles—**20 users/day (demo), 500 (development), 1,000 (nominal), 60,000 (load test)**—recording latency, throughput, total processing time, and relevant resource observations.
5. **SMART-5 — Reproducible manual demo (pre-delivery).** Ensure the scripted flow `simulate messages -> observe streaming results -> run forced batch -> inspect Gold` completes without waiting for real calendar time.
6. **SMART-6 — Engineering quality (pre-delivery).** Maintain reproducible local setup instructions, meaningful unit/integration tests for critical behavior, and CI that runs the main automated quality checks.

## Initial SLOs and SLIs

| Concern | SLI / measurement | Initial SLO | Applies to |
| --- | --- | --- | --- |
| Provisional scoring latency | `serving.provisional_scores.calculated_at - serving.provisional_scores.kafka_timestamp` | **p95 ≤ 5 s; p99 ≤ 10 s** | Nominal profile |
| Bronze ingestion completeness | Kafka deliveries successfully processed vs. Bronze records | **100%** | Functional/integration tests |
| Provisional idempotency | Provisional rows per logical event | **Exactly 1** | Duplicate-delivery tests |
| Accelerated-load throughput | Fully processed session events / elapsed seconds | **≥ 100 events/s average** | 60,000-user profile |
| Accelerated-load completion | End-to-end processing time for 60,000 sessions | **≤ 10 min** | 60,000-user profile |
| Daily batch runtime | Batch start to successful Gold replacement | **≤ 10 min** | 60,000-user profile |
| Gold reprocessing idempotency | Functional diff after rerunning same business date | **No change; 0 duplicate logical rows** | Batch integration tests |
| Recorded-demo flow | 20-user simulation to inspectable forced-batch Gold result (infra already running) | **≤ 3 min** | Demo profile |

- Do not require the nominal scoring-latency SLO during the intentionally accelerated 60,000-session load test. That profile validates throughput, integrity, and completion time.

### Latency evidence (first post-hoc measurement, 2026-09-13)

- Re-measured from the persisted `serving.provisional_scores` table (raw `measure.out` logs were not committed), using the same SQL as `scripts/acceptance-measure.sh`. Detailed evidence: `docs/evidence/20260913T234600Z_latency_remeasurement.md`.
- Nominal profile results: 2026-09-16 → p95 21.2 s / p99 21.2 s; 2026-09-17 (500 ms pacing) → p95 9.3 s / p99 10.0 s. Both **FAIL** the p95 ≤ 5 s target.
- The SLI spans Kafka produce → provisional write and therefore includes micro-batch queue wait. Idle/small runs show 30–50 ms end-to-end latency; the gap under sustained arrival is dominated by per-batch fixed costs (Python UDFs, several Spark jobs per batch, two S3A parquet appends, per-batch JDBC connect).
- The SLO remains unchanged; recalibrating it for the reference workstation is an open decision to be discussed, not silently applied.

## Evidence tracking

- Measurement plans, executed commands, captured metrics, and PASS/FAIL conclusions must be recorded in this document (or linked artifacts) as the implementation matures.
- The TFM memory should summarize the SMART objectives, validation methodology, and principal measured outcomes while referencing this file for detailed evidence.

## Quality and TFM constraints

- Maintain Git/GitHub workflows with reproducible local setup instructions.
- Provide reasonable unit-test coverage plus integration tests for critical pipeline behavior.
- Ensure CI runs the main automated quality checks.
- Prioritize code quality/readability and keep decisions explainable in the thesis deliverables.
- Use only generated/simulated data unless the user explicitly approves real personal data ingestion.

Minimum planned tests:

- normalization
- dictionary validation
- rhyme validation
- duplicate words in a session
- empty session handling
- provisional scoring
- Kafka technical retry/idempotency
- Bronze retaining physical duplicates
- Silver tolerating structured technical duplicates across microbatches
- PostgreSQL provisional-score idempotency under duplicate delivery
- Batch deduplication by `event_id` before Gold calculations
- First-accepted-event-wins enforcement for multiple distinct `event_id`s per user/day
- Gold reprocessing/idempotency once implemented
