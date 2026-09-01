from __future__ import annotations

import logging
import signal
import sys
import time

from pyspark.sql import SparkSession

from bertsonal_spark.config import StreamingConfig


logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )


def build_spark_session(config: StreamingConfig) -> SparkSession:
    builder = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", config.checkpoint_root)
    )
    return builder.getOrCreate()


def main() -> int:
    _setup_logging()
    config = StreamingConfig()
    spark = build_spark_session(config)

    logger.info(
        "Spark streaming scaffold running. Kafka=%s, MinIO=%s, Postgres=%s:%s",
        config.kafka_bootstrap_servers,
        config.minio_endpoint,
        config.postgres_host,
        config.postgres_port,
    )

    should_run = True

    def _handle_signal(signum, frame):  # type: ignore[override]
        nonlocal should_run
        logger.info("Received signal %s; stopping streaming stub", signum)
        should_run = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while should_run:
            time.sleep(30)
    finally:
        logger.info("Stopping Spark session")
        spark.stop()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
