from dataclasses import dataclass
from datetime import date


@dataclass
class SimulatorConfig:
    users: int
    business_date: date
    rhyme: str
    rhyme_id: str
    words_mean: float
    words_stddev: float
    empty_session_rate: float
    random_word_rate: float
    typo_rate: float
    delay_ms: int


@dataclass
class KafkaSettings:
    bootstrap_servers: str
    topic: str
