from __future__ import annotations

from bertsonal_simulator.kafka_client import KafkaPublisher


class DummyProducer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.sent = []
        self.flushed = False
        self.closed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def test_kafka_publisher_sends_messages(monkeypatch):
    monkeypatch.setattr(
        "bertsonal_simulator.kafka_client.KafkaProducer",
        lambda **kwargs: DummyProducer(**kwargs),
    )

    publisher = KafkaPublisher("kafka:9092", "session-events")
    publisher.publish("user-1", {"foo": "bar"})
    publisher.close()

    dummy = publisher.producer
    assert dummy.kwargs["bootstrap_servers"] == "kafka:9092"
    assert dummy.sent == [("session-events", "user-1", {"foo": "bar"})]
    assert dummy.flushed is True
    assert dummy.closed is True
