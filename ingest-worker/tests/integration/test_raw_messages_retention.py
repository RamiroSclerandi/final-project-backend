"""Retention of `raw_messages`: `purge_raw_messages()` and its rollback.

Processed rows are kept 7 days and every other row (unprocessed or with an
error) 15 days. The function runs from pg_cron in Supabase; the schedule itself
is asserted in test_pg_cron_schedule.py, because the shared harness image has
no pg_cron.

The database is shared across the session, so each test tags its rows with a
unique topic and asserts only on those.
"""

import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_SUPABASE_DIR = Path(__file__).resolve().parents[3] / "supabase"
_MIGRATION = _SUPABASE_DIR / "migrations" / "20261002120000_raw_messages_retention.sql"
_ROLLBACK = _SUPABASE_DIR / "rollbacks" / "20261002120000_raw_messages_retention.sql"


def _archive(
    client: Client, topic: str, *, age: timedelta, processed: bool, error: str | None = None
) -> None:
    client.table("raw_messages").insert(
        {
            "topic": topic,
            "payload": {"age_days": age.days},
            "source": "hivemq",
            "received_at": (datetime.now(UTC) - age).isoformat(),
            "processed": processed,
            "error": error,
        }
    ).execute()


def _remaining_ages(client: Client, topic: str) -> list[int]:
    rows = client.table("raw_messages").select("payload").eq("topic", topic).execute().data
    return sorted(int(row["payload"]["age_days"]) for row in rows)  # type: ignore[index, call-overload]


@pytest.fixture
def topic() -> str:
    return f"dl/v1/{secrets.token_hex(6).upper()}/data"


def test_processed_rows_older_than_seven_days_are_purged(
    service_role_client: Client, query_scalar: Callable[[str], str], topic: str
) -> None:
    _archive(service_role_client, topic, age=timedelta(days=8), processed=True)
    _archive(service_role_client, topic, age=timedelta(days=6), processed=True)

    query_scalar("SELECT purge_raw_messages()")

    assert _remaining_ages(service_role_client, topic) == [6]


def test_unprocessed_and_failed_rows_are_kept_fifteen_days(
    service_role_client: Client, query_scalar: Callable[[str], str], topic: str
) -> None:
    _archive(service_role_client, topic, age=timedelta(days=16), processed=False)
    _archive(service_role_client, topic, age=timedelta(days=14), processed=False)
    _archive(service_role_client, topic, age=timedelta(days=10), processed=False, error="bad")

    query_scalar("SELECT purge_raw_messages()")

    assert _remaining_ages(service_role_client, topic) == [10, 14]


@pytest.mark.parametrize("client_fixture", ["anon_client", "authenticated_client"])
def test_clients_cannot_call_the_purge(client_fixture: str, request: pytest.FixtureRequest) -> None:
    client: Client = request.getfixturevalue(client_fixture)

    with pytest.raises(APIError):
        client.rpc("purge_raw_messages").execute()


def test_rollback_removes_the_purge_and_reapplying_restores_it(
    apply_sql: Callable[[Path], None], query_scalar: Callable[[str], str]
) -> None:
    function_count = "SELECT count(*) FROM pg_proc WHERE proname = 'purge_raw_messages'"

    apply_sql(_ROLLBACK)
    try:
        assert query_scalar(function_count) == "0"
    finally:
        apply_sql(_MIGRATION)

    assert query_scalar(function_count) == "1"
