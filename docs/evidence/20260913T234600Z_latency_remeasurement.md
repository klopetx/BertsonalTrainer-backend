## Acceptance Measurement (post-hoc)

- measurement_id: 20260913T234600Z
- commit at measurement: 148c31a
- measured_at: 2026-09-13T23:46:00Z
- method: SQL `percentile_cont` over `serving.provisional_scores` (same query as `scripts/acceptance-measure.sh`), executed against the running compose PostgreSQL.
- reason: raw `measure.out` logs of the recorded acceptance runs were not committed (`evidence/runs/` is gitignored); the persisted serving table still holds `kafka_timestamp`, so the latency SLI was re-measured from live data.

### Measurements (per business_date)

| business_date | rows | p95_s | p99_s | avg_s | max_s | Profile (from docs/evidence) | SLO verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-09-08 | 20 | 55.881 | 55.881 | 55.878 | 55.881 | (manual, pre-evidence) | n/a — startup artifact (streaming started ~1 min after produce) |
| 2026-09-09 | 20 | 0.043 | 0.043 | 0.041 | 0.043 | (manual, pre-evidence) | n/a |
| 2026-09-10 | 20 | 0.030 | 0.031 | 0.026 | 0.031 | (manual, pre-evidence) | n/a |
| 2026-09-12 | 20 | 0.030 | 0.031 | 0.027 | 0.031 | demo | PASS (0.03 s) |
| 2026-09-13 | 500 | 24.325 | 24.336 | 22.443 | 24.339 | dev | SLO applies to nominal only |
| 2026-09-14 | 10000 | 15.774 | 16.034 | 13.220 | 16.121 | dev (intermediate scale) | SLO applies to nominal only |
| 2026-09-15 | 60000 | 29.603 | 31.437 | 13.712 | 31.715 | load | Exempt by design (accelerated load) |
| 2026-09-16 | 1000 | 21.229 | 21.249 | 18.748 | 21.264 | nominal | **FAIL** (p95 ≤ 5 s; p99 ≤ 10 s) |
| 2026-09-17 | 1000 | 9.312 | 10.014 | 4.954 | 11.275 | nominal (500 ms pacing) | **FAIL** (p95 9.3 s; p99 10.014 s) |
| 2026-09-18 | 5 | 0.041 | 0.041 | 0.040 | 0.041 | (manual) | n/a |
| 2026-09-19 | 6 | 0.047 | 0.047 | 0.041 | 0.047 | (manual) | n/a |

### Interpretation

- The SLI (`calculated_at − kafka_timestamp`) spans Kafka produce → provisional-score write, so it includes micro-batch queue wait, not just scoring compute.
- Idle/small runs (2026-09-09/10/12/18/19) show 30–50 ms end-to-end latency: the pipeline itself is near-instant without backlog.
- Sustained arrival rates make the per-batch fixed cost the bottleneck: each silver micro-batch runs multiple Spark jobs (`rdd.isEmpty()` checks, cache materializations, two S3A parquet appends to MinIO, a driver-side `collect()`), evaluates three Python UDFs per word row, and opens a fresh JDBC connection (see `services/spark/bertsonal_spark/streaming/main.py`).
- Observed batch-cycle equilibrium: ~5 s at 2 events/s (2026-09-17), ~20 s at 27 events/s (2026-09-16).
- No trigger interval is configured; the streaming loop runs as fast as each batch completes.

### Conclusion

- The provisional-scoring latency SLO (p95 ≤ 5 s; p99 ≤ 10 s, nominal profile) was **not met** on the recorded nominal runs.
- The SLO remains unchanged; recalibration for the reference workstation is an open decision (see `AGENTS.md` rules on evidence and target changes).
