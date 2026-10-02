"""Daily aggregate buckets follow the Argentine calendar day, and their rollback."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from supabase import Client

from ingest.sink.supabase_sink import SupabaseStore

pytestmark = pytest.mark.integration

_SUPABASE_DIR = Path(__file__).resolve().parents[3] / "supabase"
_LOCAL_DAY = "20261002150000_daily_buckets_local_day.sql"


def _sensor_with_readings(
    store: SupabaseStore, service_role_client: Client, mac: str, instants: list[datetime]
) -> str:
    device = store.insert_device(mac, name=f"Nodo {mac}")
    sensor_type = store.select_sensor_type("temperature", "degC")
    assert sensor_type is not None
    sensor = store.insert_sensor(device.id, sensor_type.id, "bmp280", "")
    assert sensor is not None
    service_role_client.table("measurements").insert(
        [
            {"sensor_id": sensor.id, "value": 20.0, "timestamp": instant.isoformat()}
            for instant in instants
        ]
    ).execute()
    return sensor.id


def _daily_buckets(query_scalar: Callable[[str], str], sensor_id: str) -> list[str]:
    query_scalar("REFRESH MATERIALIZED VIEW mv_measurements_daily")
    rows = query_scalar(
        "SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') || '|' || sample_count"
        f" FROM mv_measurements_daily WHERE sensor_id = '{sensor_id}' ORDER BY bucket"
    )
    return rows.splitlines()


def test_readings_split_at_argentine_midnight_not_utc_midnight(
    store: SupabaseStore,
    service_role_client: Client,
    query_scalar: Callable[[str], str],
    unique_mac: str,
) -> None:
    # 23:30 on Oct 2 and 01:00 on Oct 3 in Argentina (UTC-3): one UTC day, two local days.
    instants = [
        datetime(2026, 10, 3, 2, 30, tzinfo=UTC),
        datetime(2026, 10, 3, 4, 0, tzinfo=UTC),
    ]
    sensor_id = _sensor_with_readings(store, service_role_client, unique_mac, instants)

    buckets = _daily_buckets(query_scalar, sensor_id)

    assert buckets == ["2026-10-02 03:00|1", "2026-10-03 03:00|1"]


def test_daily_view_groups_by_the_argentine_calendar_day(
    query_scalar: Callable[[str], str],
) -> None:
    definition = query_scalar("SELECT pg_get_viewdef('mv_measurements_daily'::regclass)")

    assert "America/Argentina/Buenos_Aires" in definition


def test_rollback_restores_utc_buckets_and_reapplying_restores_local_days(
    apply_sql: Callable[[Path], None], query_scalar: Callable[[str], str]
) -> None:
    definition = "SELECT pg_get_viewdef('mv_measurements_daily'::regclass)"

    apply_sql(_SUPABASE_DIR / "rollbacks" / _LOCAL_DAY)
    try:
        assert "America/Argentina/Buenos_Aires" not in query_scalar(definition)
    finally:
        apply_sql(_SUPABASE_DIR / "migrations" / _LOCAL_DAY)

    assert "America/Argentina/Buenos_Aires" in query_scalar(definition)
