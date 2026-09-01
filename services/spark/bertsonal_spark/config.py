from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class StreamingConfig:
    app_name: str = "bertsonaltrainer-streaming"
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "session-events")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user: str = os.getenv("POSTGRES_USER", "bertsonal")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "bertsonal_pw")
    postgres_db: str = os.getenv("POSTGRES_DB", "bertsonal")
    checkpoint_root: str = os.getenv("SPARK_CHECKPOINT_ROOT", "/opt/app/checkpoints/streaming")
