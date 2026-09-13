# Metabase (Optional, Isolated)

Metabase is not part of the core pipeline. To avoid interfering with
Kafka/Spark/MinIO/Postgres jobs, its services (`metabase`, `metabase-db`) live
in the main `compose.yaml` but are started only on demand.

The intent is:

1. Run Metabase independently from the data services.
2. Manually connect it to the project's PostgreSQL as a read-only analytics
   source.

## Start / stop

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

## Connect Metabase to the project Postgres (Gold tables)

Metabase runs in a container, while the project's Postgres may run in another
Compose stack (this repo's `compose.yaml`) or directly on the host.

In Metabase:

1. Admin settings -> Databases -> Add database
2. Choose `PostgreSQL`
3. Use the connection settings below.

### If the project's Postgres is running via this repo's `compose.yaml`

By default, `compose.yaml` publishes Postgres on the host at `localhost:5432`.
From inside the Metabase container, reaching the host uses:

- Podman: `host.containers.internal`
- Docker Desktop: `host.docker.internal`

Suggested parameters:

- Host: `host.containers.internal` (Podman) or `host.docker.internal` (Docker)
- Port: `5432`
- Database name: `bertsonal` (default in `compose.yaml`)
- Username: the `POSTGRES_USER` value from `.env` (default `bertsonal`)
- Password: the `POSTGRES_PASSWORD` value from `.env` (default `bertsonal_pw`)

Once connected, browse the Gold layer tables:

- `gold.daily_scores`
- `gold.rhyme_daily_metrics`
- `gold.session_word_metrics`

## Recommended Metabase questions (built on Gold)

These are all doable using the query builder or SQL cards.

### Daily leaderboard

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

### Participation over time

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

### Most repeated words (proxy for low originality)

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

## Safety (optional): use a read-only DB user

Metabase only needs `SELECT` permissions for BI.

You can create a read-only role manually in the project's Postgres and then use
it in Metabase. Example (run as a privileged user):

```sql
CREATE ROLE metabase_ro LOGIN PASSWORD 'change_me';
GRANT CONNECT ON DATABASE bertsonal TO metabase_ro;

GRANT USAGE ON SCHEMA gold TO metabase_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO metabase_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
GRANT SELECT ON TABLES TO metabase_ro;
```

This is not automated on purpose, to avoid touching a running system.
