# Kafka Topology and Event Contract

This document captures the agreed Kafka configuration, the canonical `session-events` payload, and the rules that govern event validation, retries, and late arrivals.

## Broker and topic configuration

- Local deployment uses a single Kafka broker.
- Primary topic: `session-events`.
- Partitions: 1 (per-user ordering via `user_id` as the message key).
- Replication factor: 1.
- Retention: 3 days.
- Exactly one Kafka message represents one completed session.
- Kafka is transport/buffer storage, not the historical source of truth. Bronze in MinIO is the long-term immutable store.

## Producer and consumer roles

- Producer: the Python simulator publishes an event per completed session.
- Consumer: Spark Structured Streaming ingests from Kafka, persists to Bronze/Silver, and writes provisional scores to PostgreSQL.
- `kafka-init` must explicitly create/configure the topic so the setup remains reproducible.

## Canonical v1 payload

```json
{
  "schema_version": "1.0",
  "event_id": "uuid",
  "session_id": "uuid",
  "user_id": "string",
  "business_date": "YYYY-MM-DD",
  "rhyme_id": "string",
  "rhyme": "string",
  "started_at": "ISO-8601 UTC",
  "completed_at": "ISO-8601 UTC",
  "submitted_words": ["string"]
}
```

Rules:

- `schema_version` is mandatory; only `1.0` is supported initially.
- `event_id` is the primary idempotency identifier and must be a UUID.
- `session_id` identifies the functional session.
- `user_id` is also the Kafka key; it must be non-empty.
- `business_date` captures the logical exercise day (not ingestion time).
- `rhyme_id` and `rhyme` keep the event self-contained.
- `started_at`/`completed_at` are ISO-8601 UTC timestamps with `completed_at >= started_at`.
- `submitted_words` contains the raw user input (may be empty) and must remain unnormalized at publication.
- Derived values (validity, scores, rankings, originality) never belong in the Kafka payload.

## Contract validation

Streaming must reject malformed events without crashing the pipeline. Required checks include:

- Supported `schema_version`.
- Valid UUIDs for `event_id` and `session_id`.
- Non-empty `user_id`, `rhyme_id`, and `rhyme`.
- Valid `business_date`.
- Proper timestamps with `completed_at >= started_at`.
- `submitted_words` must be an array/list of strings.

Events that fail these rules are quarantined once that capability exists (see `docs/medallion-model.md`).

## Identity, retries, and payload hash

- `source_record_id` = `topic:partition:offset` identifies the physical Kafka delivery persisted in Bronze/Silver.
- Legitimate technical retries reuse the same `event_id` and identical payload. They appear multiple times in Bronze/Silver but collapse before Gold.
- `payload_hash` (SHA-256 of the raw payload) distinguishes a valid retry from an integrity conflict. Same `event_id` + different `payload_hash` is flagged as `INTEGRITY_CONFLICT`.
- Functional rule: one official session per user per business date. When multiple distinct `event_id` values exist for the same `user_id + business_date`, **first accepted event wins** based on Kafka arrival order (single partition ensures deterministic ordering).

## Late events and ingestion behavior

- Business timezone: `Europe/Madrid` with a 1-hour grace window after midnight. Previous-day sessions are accepted until `01:00` local time the next day.
- Events that arrive after the cutoff still land in Bronze (audit trail) but must not affect Silver, provisional scores, or Gold.
- Late or otherwise unprocessable events are classified under categories such as `LATE_EVENT`, `JSON_PARSE_ERROR`, `UNSUPPORTED_SCHEMA_VERSION`, `CONTRACT_VALIDATION_ERROR`, `BUSINESS_RULE_VIOLATION`, or `INTEGRITY_CONFLICT` when the quarantine area is implemented.

## Relationship to Bronze

- Every processed Kafka delivery is appended to Bronze with full Kafka metadata plus the untouched raw payload.
- Bronze partitioning uses ingestion date (`bronze/session-events/ingestion_date=YYYY-MM-DD/`).
- Bronze’s immutability ensures the system can be replayed or audited without depending on Kafka retention.
