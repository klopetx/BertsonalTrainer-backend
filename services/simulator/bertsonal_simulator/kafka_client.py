from __future__ import annotations

import json
import logging
from typing import Any, Dict

from kafka import KafkaProducer


logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            linger_ms=50,
        )

    def publish(self, key: str, value: Dict[str, Any]) -> None:
        logger.debug("Publishing event for user %s", key)
        self.producer.send(self.topic, key=key, value=value)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
