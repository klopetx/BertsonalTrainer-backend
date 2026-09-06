# Checking operation

### Gold: PostgreSQL

#### Listing tables

```bash
podman compose exec postgres psql -U bertsonal -d bertsonal -c "\dt serving.*"
```

```bash
podman compose exec postgres psql -U bertsonal -d bertsonal -c "\dt gold.*"
```

#### View content

```bash
podman compose exec postgres psql -U bertsonal -d bertsonal -c "select * from gold.daily_scores;"
```

```bash
podman compose exec postgres psql -U bertsonal -d bertsonal -c "select * from gold.rhyme_daily_metrics;"
```

```bash
podman compose exec postgres psql -U bertsonal -d bertsonal -c "select * from gold.session_word_metrics limit 20;"
```

### Kafka: session-events

#### List topics

```bash
podman compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

#### Peek messages

```bash
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic session-events --from-beginning --max-messages 5
```

Press `Ctrl+C` after the sample messages display to exit the consumer.

### MinIO: Bronze & Silver

#### Configure CLI alias (uses compose defaults)

```bash
podman compose run --rm minio-init mc alias set local http://minio:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin123}
```

#### List buckets & prefixes

```bash
podman compose run --rm minio-init mc ls local
podman compose run --rm minio-init mc ls local/bronze
podman compose run --rm minio-init mc ls local/silver
```

#### Inspect partitions

```bash
podman compose run --rm minio-init mc ls --recursive local/bronze/session-events
podman compose run --rm minio-init mc ls --recursive local/silver/sessions
podman compose run --rm minio-init mc ls --recursive local/silver/session-words
```

#### View or copy files

```bash
podman compose run --rm minio-init mc cat local/silver/sessions/business_date=YYYY-MM-DD/part-00000.parquet | hexdump -C
podman compose run --rm minio-init mc cp local/silver/sessions/business_date=YYYY-MM-DD/part-00000.parquet /tmp/sessions.parquet
```

Replace `business_date=YYYY-MM-DD` with an actual partition present in MinIO.
