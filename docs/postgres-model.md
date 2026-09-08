# PostgreSQL Serving and Gold Schemas

PostgreSQL provides two schemas: `serving` for provisional scores written by streaming and `gold` for definitive analytical outputs produced by the daily batch. This document details the approved tables, constraints, indexes, and idempotency strategies.

## Schema overview

```text
PostgreSQL
|-- serving
|   `-- provisional_scores
`-- gold
    |-- daily_scores
    |-- rhyme_daily_metrics
    |-- session_word_metrics
    |-- weekly_rankings
    `-- monthly_rankings
```

`gold.business_kpis` remains deliberately deferred until a concrete Metabase/dashboard need exists.

## `serving.provisional_scores`

One row per accepted session during streaming.

| Column | Type | Notes |
| --- | --- | --- |
| `event_id` | UUID | Primary key and technical idempotency key |
| `session_id` | UUID | Functional session identifier (UNIQUE) |
| `user_id` | TEXT | Simulated user (UNIQUE with `business_date`) |
| `business_date` | DATE | Logical exercise day |
| `rhyme_id` | TEXT | Daily rhyme identifier |
| `valid_word_count` | INTEGER | Distinct valid words (>= 0) |
| `provisional_score` | INTEGER | Equals `valid_word_count` in v1 |
| `kafka_timestamp` | TIMESTAMPTZ | Kafka arrival timestamp for latency calculations; nullable for forward-only migrations |
| `calculated_at` | TIMESTAMPTZ | Streaming calculation timestamp |

Constraints:

```sql
PRIMARY KEY (event_id)
UNIQUE (session_id)
UNIQUE (user_id, business_date)
CHECK (valid_word_count >= 0)
CHECK (provisional_score >= 0)
```

Additional index:

```sql
CREATE INDEX idx_provisional_scores_business_date
ON serving.provisional_scores (business_date);
```

Write policy:

- Inserts use semantics equivalent to `INSERT ... ON CONFLICT DO NOTHING`.
- A legitimate technical retry (same `event_id`, identical payload) must not insert a duplicate or update the row.
- If a different `event_id` appears for an already accepted `user_id + business_date`, the **first accepted event wins**; later attempts are ignored/quarantined.

## Gold idempotency strategy

- Gold writes use transactional **replace-by-scope** semantics:

  1. Recompute the full `business_date`, associated `week_start_date`, and `month_start_date` scopes from Silver.
  2. Inside a single transaction, delete prior rows for those scopes across all affected Gold tables.
  3. Insert the recalculated rows.

- The transaction commits only when every table update succeeds, preventing stale or duplicate rows during reprocessing.

## `gold.daily_scores`

One row per accepted user and business date after definitive deduplication.

| Column | Type | Notes |
| --- | --- | --- |
| `business_date` | DATE | Partition key; part of PK |
| `user_id` | TEXT | Part of PK |
| `session_id` | UUID | Accepted session (UNIQUE) |
| `rhyme_id` | TEXT | Daily rhyme |
| `valid_word_count` | INTEGER | Distinct valid words |
| `daily_score` | NUMERIC | Sum of `daily_word_score` across the session |
| `hardness_weighted_daily_score` | NUMERIC | `daily_score * (1 - (EDW/74)*0.6)` |
| `originality_score` | NUMERIC | Mirrors `daily_score` for downstream compatibility |
| `final_score` | NUMERIC | Equals `hardness_weighted_daily_score` in the current MVP |
| `rank_position` | INTEGER | Materialized daily rank |
| `calculated_at` | TIMESTAMPTZ | Batch calculation timestamp |

Indexes/constraints:

```sql
PRIMARY KEY (business_date, user_id)
UNIQUE (session_id)

CREATE INDEX idx_daily_scores_date_rank
ON gold.daily_scores (business_date, rank_position);
```

## `gold.session_word_metrics`

One row per valid, unique word that contributed to an accepted session.

| Column | Type | Notes |
| --- | --- | --- |
| `business_date` | DATE | Partition key; part of PK |
| `session_id` | UUID | References the accepted session |
| `user_id` | TEXT | Denormalized for convenience |
| `rhyme_id` | TEXT | Matches the day’s rhyme |
| `normalized_word` | TEXT | Normalized dictionary entry |
| `daily_repetitions` | INTEGER | Distinct other users who also submitted the word |
| `daily_word_score` | NUMERIC | `1 - (daily_repetitions / Rmax) * 0.5`, defaults to `1.0` when `Rmax = 0` |
| `daily_score` | NUMERIC | Session-level total (repeated for convenience) |
| `hardness_weighted_daily_score` | NUMERIC | Session HWDS repeated for convenience |
| `calculated_at` | TIMESTAMPTZ | Batch calculation timestamp |

`Rmax` represents the maximum `daily_repetitions` observed for the business date; unique words retain the best score when no other users submitted them.

## `gold.rhyme_daily_metrics`

One row per business day, enforcing the rule that all users share the same rhyme.

| Column | Type | Notes |
| --- | --- | --- |
| `business_date` | DATE | Primary key |
| `rhyme_id` | TEXT | Daily rhyme identifier |
| `rhyme` | TEXT | Rhyme text/suffix |
| `participants` | INTEGER | Accepted sessions, including empty ones |
| `total_valid_words` | INTEGER | Sum of distinct valid words across sessions |
| `average_valid_words_per_session` | NUMERIC | Informational metric (not used for scoring in the current model) |
| `calculated_at` | TIMESTAMPTZ | Batch calculation timestamp |

## `gold.weekly_rankings`

`week_start_date` is Monday (ISO-style). Days without participation are not counted as zero-score days when computing averages.

| Column | Type |
| --- | --- |
| `week_start_date` | DATE |
| `user_id` | TEXT |
| `total_score` | NUMERIC |
| `average_score` | NUMERIC |
| `days_played` | INTEGER |
| `rank_by_total` | INTEGER |
| `rank_by_average` | INTEGER |
| `calculated_at` | TIMESTAMPTZ |

Constraints/indexes:

```sql
PRIMARY KEY (week_start_date, user_id)

CREATE INDEX idx_weekly_rank_total
ON gold.weekly_rankings (week_start_date, rank_by_total);

CREATE INDEX idx_weekly_rank_average
ON gold.weekly_rankings (week_start_date, rank_by_average);
```

## `gold.monthly_rankings`

Uses the first calendar day of the month (`month_start_date`) rather than a `year + month` pair.

| Column | Type |
| --- | --- |
| `month_start_date` | DATE |
| `user_id` | TEXT |
| `total_score` | NUMERIC |
| `average_score` | NUMERIC |
| `days_played` | INTEGER |
| `rank_by_total` | INTEGER |
| `rank_by_average` | INTEGER |
| `calculated_at` | TIMESTAMPTZ |

Constraints/indexes:

```sql
PRIMARY KEY (month_start_date, user_id)

CREATE INDEX idx_monthly_rank_total
ON gold.monthly_rankings (month_start_date, rank_by_total);

CREATE INDEX idx_monthly_rank_average
ON gold.monthly_rankings (month_start_date, rank_by_average);
```

## Business rules enforced in PostgreSQL

- Provisional and final scores remain independently auditable; never overwrite provisional rows with final results.
- Do not introduce surrogate `BIGSERIAL` IDs where natural/business keys are sufficient.
- Streaming inserts plus database constraints enforce idempotency before the batch runs.
- Batch processing applies the **first accepted event wins** policy using deterministic Kafka ordering (single partition, `user_id` key). Later distinct events for the same user/day must not replace the accepted session.
- Gold tables materialize both cumulative and average metrics (especially for weekly/monthly rankings) to simplify downstream BI consumption.
