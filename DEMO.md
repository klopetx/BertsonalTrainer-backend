# Demo Runbook (Video Script)

This is a command-by-command runbook for a demo video of the pipeline:

`simulator -> Kafka -> Spark Structured Streaming -> MinIO (Bronze/Silver) -> Postgres (serving) -> (optional) Metabase -> Spark batch -> Postgres (gold) -> Metabase`.

Run commands from the repo root in **Git Bash** (recommended) so the `./scripts/*.sh` wrappers work.

## 0) Demo Variables (Pick Fresh Values)

Pick a business date that has no rows yet in `serving.provisional_scores` (first accepted event wins, so a date with prior data will show no new provisional scores) and that is not in the past (past dates are classified as late events). Check existing dates first:

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c \
  "SELECT DISTINCT business_date FROM serving.provisional_scores ORDER BY business_date;"
```

```bash
BUSINESS_DATE=$(date +%F)
USERS=5
RHYME=ari
RHYME_ID=R001
SEED=42
```

## 1) Start Infrastructure

```bash
./scripts/infra-up.sh
podman compose ps
```

Optional: tail logs in a separate terminal tab:

```bash
./scripts/logs.sh kafka
```

## 2) Start Spark Streaming

```bash
./scripts/streaming-up.sh
podman compose ps
```

Tail streaming logs (separate terminal tab is best for video):

```bash
./scripts/logs.sh spark-streaming
```

## 3) Show Kafka Topic State (Before Producing)

Capture the current end offset, then later consume only the newly produced events.

```bash
START_OFFSET=$(podman compose exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic session-events \
  | awk -F: '{print $3}')

echo "Kafka end offset BEFORE produce: ${START_OFFSET}"
```

## 4) Produce Events (Simulator)

```bash
./scripts/simulate.sh \
  --users "$USERS" \
  --business-date "$BUSINESS_DATE" \
  --rhyme "$RHYME" \
  --rhyme-id "$RHYME_ID" \
  --seed "$SEED" \
  --delay-ms 0
```

## 5) Show the New Kafka Messages

Consume starting from the offset captured in step 3.

```bash
podman compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic session-events \
  --partition 0 \
  --offset "$START_OFFSET" \
  --max-messages "$USERS" \
  --property print.key=true \
  --property print.timestamp=true
```

Tip for narration: point out `user_id` is the message key, preserving ordering on the single partition.

## 6) Wait Until Streaming Writes to Postgres (Provisional Scores)

This waits until `serving.provisional_scores` has at least `USERS` rows for the business date.

```bash
./scripts/acceptance-wait.sh "$BUSINESS_DATE" "$USERS" 300 5
```

## 7) Show Postgres Serving Table

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c \
  "SELECT user_id, business_date, rhyme_id, valid_word_count, provisional_score, kafka_timestamp, calculated_at\
   FROM serving.provisional_scores\
   WHERE business_date = DATE '${BUSINESS_DATE}'\
   ORDER BY user_id;"
```

## 8) Show MinIO Bronze (Raw Payloads)

Bronze is stored as **Parquet** (immutable append-only) but it contains the untouched Kafka payload in `raw_payload`.
This command reads the latest few Bronze rows and prints the JSON payload.

```bash
BUSINESS_DATE="$BUSINESS_DATE" podman compose exec -T spark-streaming python - <<'PY'
import os

from pyspark.sql import functions as F

from bertsonal_spark.common.spark_utils import build_spark_session, configure_s3
from bertsonal_spark.config import StreamingConfig

cfg = StreamingConfig()
spark = build_spark_session("demo-inspect-bronze")
configure_s3(spark, cfg.minio_endpoint, cfg.minio_access_key, cfg.minio_secret_key)

df = spark.read.parquet(cfg.bronze_base_path).orderBy(F.col("ingested_at").desc())

df.select(
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    F.col("raw_payload").cast("string").alias("raw_json"),
).show(2, truncate=False)

spark.stop()
PY
```

## 9) Show MinIO Silver (Two Datasets)

Silver is structured Parquet, partitioned by `business_date`, and split into:

- `silver/sessions` (one row per ingested session event)
- `silver/session-words` (one row per submitted word with validation reasons)

```bash
BUSINESS_DATE="$BUSINESS_DATE" podman compose exec -T spark-streaming python - <<'PY'
import os

from pyspark.sql import functions as F

from bertsonal_spark.common.spark_utils import build_spark_session, configure_s3
from bertsonal_spark.config import StreamingConfig

business_date = os.environ["BUSINESS_DATE"]

cfg = StreamingConfig()
spark = build_spark_session("demo-inspect-silver")
configure_s3(spark, cfg.minio_endpoint, cfg.minio_access_key, cfg.minio_secret_key)

sessions_path = f"{cfg.silver_sessions_path}/business_date={business_date}"
words_path = f"{cfg.silver_words_path}/business_date={business_date}"

sessions = spark.read.parquet(sessions_path)
words = spark.read.parquet(words_path)

print("\\nSilver sessions (sample):")
sessions.select(
    "event_id",
    "session_id",
    "user_id",
    "business_date",
    "submitted_word_count",
    "valid_word_count",
    "invalid_word_count",
    "kafka_timestamp",
).orderBy("user_id").show(20, truncate=False)

print("\\nSilver session-words (show validity/cleaning):")
words.select(
    "session_id",
    "user_id",
    "word_index",
    "original_word",
    "normalized_word",
    "is_valid",
    "validation_reason",
).orderBy("user_id", "session_id", "word_index").show(50, truncate=False)

print("\\nInvalid word breakdown:")
words.groupBy("validation_reason").agg(F.count("*").alias("rows")).orderBy(F.desc("rows")).show(50, truncate=False)

spark.stop()
PY
```

## 10) Start Metabase (Optional) and Show "Nothing Yet" in Gold

Metabase is a standalone compose stack:

```bash
podman compose -f infra/metabase/compose.metabase.yaml up -d
```

Open Metabase:

- http://localhost:3000

Before the batch runs, Gold should be empty/non-existent. Show it in SQL:

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c "\\dt gold.*"
```

In Metabase:

1. Connect to Postgres on `host.containers.internal:5432` (Podman)
2. Browse schemas: `gold` should be empty/non-existent.

## 11) Force the Spark Batch

For a demo video, use `--force` (bypasses cutoff guardrails).

```bash
./scripts/batch-run.sh --business-date "$BUSINESS_DATE" --force
```

## 12) Show Postgres Gold Tables (After Batch)

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c "\\dt gold.*"
```

Daily leaderboard:

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c \
  "SELECT business_date, rank_position, user_id, final_score, daily_score, hardness_weighted_daily_score\
   FROM gold.daily_scores\
   WHERE business_date = DATE '${BUSINESS_DATE}'\
   ORDER BY rank_position\
   LIMIT 20;"
```

Rhyme/day metrics:

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c \
  "SELECT business_date, rhyme_id, rhyme, participants, total_valid_words, average_valid_words_per_session\
   FROM gold.rhyme_daily_metrics\
   WHERE business_date = DATE '${BUSINESS_DATE}';"
```

Word originality metrics:

```bash
podman compose exec -T postgres psql -U bertsonal -d bertsonal -c \
  "SELECT normalized_word, AVG(daily_repetitions) AS avg_daily_repetitions, COUNT(*) AS occurrences\
   FROM gold.session_word_metrics\
   WHERE business_date = DATE '${BUSINESS_DATE}'\
   GROUP BY normalized_word\
   ORDER BY avg_daily_repetitions DESC, occurrences DESC\
   LIMIT 20;"
```

## 13) Refresh Metabase and Show Metrics

In Metabase:

1. Admin -> Databases -> (your Postgres) -> Sync database schema now
2. Browse `gold.*` tables
3. Show the same leaderboard/metrics queries (query builder or SQL cards)

## 14) Cleanup (After Recording)

Stop the main stack:

```bash
./scripts/down.sh
```

Stop Metabase:

```bash
podman compose -f infra/metabase/compose.metabase.yaml down
```
