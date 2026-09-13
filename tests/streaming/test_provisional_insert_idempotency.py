from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import execute_values  # noqa: E402

from bertsonal_spark.streaming.main import (  # noqa: E402
    PROVISIONAL_INSERT_SQL,
    _ensure_provisional_table,
)

TEST_DB = "bertsonal_test_provisional"


def _connect(dbname: str):
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_TEST_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_TEST_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "bertsonal"),
        password=os.environ.get("POSTGRES_PASSWORD", "bertsonal_pw"),
        dbname=dbname,
    )


@pytest.fixture()
def provisional_db():
    try:
        admin = _connect(os.environ.get("POSTGRES_DB", "bertsonal"))
    except psycopg2.OperationalError as exc:
        pytest.skip(f"local Postgres not reachable: {exc}")

    try:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB};")
            cur.execute(f"CREATE DATABASE {TEST_DB};")
    finally:
        admin.close()

    conn = _connect(TEST_DB)
    try:
        _ensure_provisional_table(conn)
        conn.commit()
        yield conn
    finally:
        conn.close()
        admin = _connect(os.environ.get("POSTGRES_DB", "bertsonal"))
        try:
            admin.autocommit = True
            with admin.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB};")
        finally:
            admin.close()


def _payload(event_id: str, session_id: str, user_id: str) -> tuple:
    kafka_ts = datetime(2026, 9, 18, 20, 0, 0, tzinfo=timezone.utc)
    calculated_at = datetime(2026, 9, 18, 20, 0, 5, tzinfo=timezone.utc)
    return (
        event_id,
        session_id,
        user_id,
        date(2026, 9, 18),
        "R001",
        3,
        3,
        kafka_ts,
        calculated_at,
    )


def test_first_accepted_event_wins_for_same_user_and_day(provisional_db):
    event_a = str(uuid.uuid4())
    event_b = str(uuid.uuid4())
    session_a = str(uuid.uuid4())
    unrelated_user_event = str(uuid.uuid4())

    with provisional_db:
        with provisional_db.cursor() as cur:
            execute_values(
                cur,
                PROVISIONAL_INSERT_SQL,
                [_payload(event_a, session_a, "user_00001")],
            )

            execute_values(
                cur,
                PROVISIONAL_INSERT_SQL,
                [_payload(event_b, str(uuid.uuid4()), "user_00001")],
            )

            execute_values(
                cur,
                PROVISIONAL_INSERT_SQL,
                [_payload(event_a, session_a, "user_00001")],
            )

            execute_values(
                cur,
                PROVISIONAL_INSERT_SQL,
                [_payload(unrelated_user_event, str(uuid.uuid4()), "user_00002")],
            )

    with provisional_db.cursor() as cur:
        cur.execute(
            "SELECT event_id, user_id FROM serving.provisional_scores ORDER BY user_id;"
        )
        rows = cur.fetchall()

    assert rows == [
        (event_a, "user_00001"),
        (unrelated_user_event, "user_00002"),
    ]
