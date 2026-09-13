# Metabase (Optional) Quickstart

This repo keeps Metabase **out of the core pipeline** to avoid interfering with
Kafka/Spark/MinIO/Postgres jobs. Its services (`metabase`, `metabase-db`) live
in the main `compose.yaml` as **opt-in** services started only on demand.

You will:

1. Start Metabase on demand.
2. Connect it to the project's PostgreSQL as a **read-only** analytics source.
3. Build dashboards on the `gold.*` tables.

## 1) Start Metabase

From the repo root:

```bash
podman compose up -d metabase
```

Open Metabase:

- With a podman machine on WSL, published ports are not forwarded to Windows
  `localhost`; get the URL with `./scripts/metabase-url.sh` (e.g.
  `http://172.26.32.83:3000`). On setups where `localhost` works, use
  `http://localhost:3000`.

Stop only Metabase:

```bash
podman compose stop metabase metabase-db
```

Notes:

- Metabase uses its own metadata Postgres (`metabase-db`) published on `localhost:5433`
  to avoid clashing with the project's Postgres (usually `localhost:5432`).
- Your project pipeline keeps running unchanged.

## 2) Ensure Gold tables exist

Metabase visualizations should be built on Gold tables populated by the daily
batch.

Typical flow:

```bash
./scripts/streaming-up.sh
./scripts/simulate.sh --users 2 --business-date 2026-09-03 --rhyme ari --rhyme-id R001 --seed 42
./scripts/batch-run.sh --business-date 2026-09-03 --force
```

Confirm tables:

```bash
DB_USER=$(grep -m1 '^POSTGRES_USER=' .env | cut -d= -f2 | tr -d '\r'); DB_USER=${DB_USER:-bertsonal}
podman compose exec postgres psql -U "$DB_USER" -d bertsonal -c "\dt gold.*"
```

Expected in the MVP:

- `gold.daily_scores`
- `gold.rhyme_daily_metrics`
- `gold.session_word_metrics`

## 3) Connect Metabase to the project's PostgreSQL

Metabase runs in a container. The project's Postgres (from this repo's
`compose.yaml`) is usually published on the host at `localhost:5432`.

From inside a container, reaching the host uses:

- Podman: `host.containers.internal`
- Docker Desktop: `host.docker.internal`

In Metabase:

1. Admin settings -> Databases -> Add database
2. Choose `PostgreSQL`
3. Enter:

- Host: `host.containers.internal` (Podman) or `host.docker.internal` (Docker)
- Port: `5432`
- Database name: `bertsonal` (default)
- Username: the `POSTGRES_USER` value from `.env` (default `bertsonal`)
- Password: the `POSTGRES_PASSWORD` value from `.env` (default `bertsonal_pw`)

Then browse schemas:

- `gold` (dashboards)
- `serving` (optional comparisons vs provisional)

## 4) Recommended dashboards (Gold)

### A) Daily overview (time series)

Use `gold.rhyme_daily_metrics`:

- Participants over time
- Avg valid words per session over time
- Total valid words over time

SQL card:

```sql
SELECT
  business_date,
  participants,
  total_valid_words,
  average_valid_words_per_session
FROM gold.rhyme_daily_metrics
WHERE business_date BETWEEN {{start_date}} AND {{end_date}}
ORDER BY business_date;
```

### B) Daily leaderboard

Use `gold.daily_scores`:

SQL card:

```sql
SELECT
  business_date,
  rank_position,
  user_id,
  final_score,
  daily_score,
  hardness_weighted_daily_score
FROM gold.daily_scores
WHERE business_date = {{business_date}}
ORDER BY rank_position
LIMIT 10;
```

### C) Originality / repeated words

Use `gold.session_word_metrics`:

```sql
SELECT
  normalized_word,
  AVG(daily_repetitions) AS avg_daily_repetitions,
  COUNT(*) AS occurrences
FROM gold.session_word_metrics
WHERE business_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY normalized_word
ORDER BY avg_daily_repetitions DESC, occurrences DESC
LIMIT 25;
```

## 5) Optional: create a read-only DB user

Metabase only needs `SELECT` permissions.

Run in the project's Postgres as a privileged user:

```sql
CREATE ROLE metabase_ro LOGIN PASSWORD 'change_me';
GRANT CONNECT ON DATABASE bertsonal TO metabase_ro;

GRANT USAGE ON SCHEMA gold TO metabase_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO metabase_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
GRANT SELECT ON TABLES TO metabase_ro;
```

Then connect Metabase using `metabase_ro` instead of `bertsonal`.

## MVP caveat

In the current MVP implementation:

- `final_score` equals `hardness_weighted_daily_score`.
- No separate `rhyme_difficulty` column: rhyme difficulty is modeled via hardness (dictionary-size multiplier).
