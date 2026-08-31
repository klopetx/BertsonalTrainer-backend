# Batch Processing, Business-Day Close, and Scoring

This document describes the agreed daily-close policy, batch responsibilities, originality/difficulty metrics, ranking semantics, and scheduling rules. It also reiterates that the final scoring formula remains an open decision.

## Business-day policy and late events

- Business timezone: `Europe/Madrid`.
- Logical day changes at `00:00` local time.
- Previous-day sessions have a one-hour grace window after midnight; the acceptance cutoff for business date `D` is `01:00` local time on `D+1`.
- Events received after the cutoff are considered late. They may still be appended to Bronze but must not modify Silver, provisional scores, or Gold. Late events will eventually be labeled `LATE_EVENT` in the quarantine area.
- Between `00:00` and `01:00`, streaming may ingest both the new day’s sessions and late-but-still-valid sessions for the previous day.

## Batch responsibilities

The Spark batch job consumes Silver (not Kafka/Bronze) and is responsible for:

- Definitive technical deduplication by `event_id` and `payload_hash`.
- Enforcing the **first accepted event wins** rule for each `user_id + business_date` using Kafka arrival ordering.
- Calculating daily word-frequency/originality metrics.
- Computing rhyme difficulty from observed performance.
- Writing `gold.daily_scores`, `gold.rhyme_daily_metrics`, `gold.weekly_rankings`, and `gold.monthly_rankings` via transactional replace-by-scope semantics.
- Materializing daily/weekly/monthly rankings with both cumulative and average fields.
- Preparing data for future KPIs/Metabase dashboards (while `gold.business_kpis` stays deferred).

## Originality (agreed concept)

- Consider only valid, distinct words per session.
- A word is more original when fewer distinct users submitted the same normalized word for the same business day/rhyme.
- Exact scoring weights remain unresolved; batch must emit the necessary frequency statistics so the final formula can be applied once approved.

## Rhyme difficulty (agreed concept)

```
average_valid_words_per_session =
    total distinct valid words contributed across sessions
    / total completed sessions (including zero-valid-word sessions)
```

- Lower averages indicate harder rhymes; higher averages indicate easier rhymes.
- The eventual transformation from this metric to a scoring weight is still pending approval.

## Rankings

- Daily rankings derive directly from `gold.daily_scores` and include `rank_position` for the final score.
- Weekly rankings use `week_start_date` = Monday (ISO-style) and store `total_score`, `average_score`, `days_played`, `rank_by_total`, and `rank_by_average`. Days without participation are excluded from the denominator when computing averages.
- Monthly rankings use `month_start_date` = first calendar day of the month and store the same cumulative/average fields plus rank columns.

## Final scoring formula (open decision)

- Do **not** invent weights or mathematical combinations until the user approves a final formula.
- The batch must remain ready to incorporate originality and rhyme-difficulty factors once the formula is defined.

## Cron scheduling and manual controls

- Automatic execution uses a lightweight cron-based scheduler service that reuses the Spark batch-capable image. The scheduler runs `spark-submit` for `bertsonal_spark/batch/main.py` and never shells out to Compose.
- Normal automatic runs occur only after the `01:00 Europe/Madrid` cutoff. A small readiness delay/check after the cutoff remains an open decision and cannot be hard-coded without approval.
- Manual execution uses `./scripts/batch-run.sh --business-date YYYY-MM-DD`. A `--force` flag exists solely for local development/tests/demos when the real cutoff has not passed; it must remain explicit and clearly logged.
- Re-running the batch for the same business date must be idempotent thanks to Gold’s replace-by-scope writes.

## Interaction with streaming

- Streaming inserts provisional scores using `ON CONFLICT DO NOTHING` semantics; this protects idempotency until the batch runs.
- The batch is the definitive step for business-level deduplication and final ranking calculations.
- Persistent streaming checkpoints (see `docs/medallion-model.md`) ensure the daily batch can rely on Silver completeness up to the accepted offset.
