"""`public.get_sensor_series`: on-demand bucketing of one sensor's readings, and its rollback."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from postgrest.exceptions import APIError
from supabase import Client

from ingest.sink.supabase_sink import SupabaseStore

pytestmark = pytest.mark.integration

_SUPABASE_DIR = Path(__file__).resolve().parents[3] / "supabase"
_SERIES = "20261006120000_get_sensor_series.sql"
_SIGNATURE = "public.get_sensor_series(uuid, timestamptz, timestamptz, text)"
_PERMISSION_DENIED = "42501"
_START = datetime(2026, 10, 3, 13, 0, tzinfo=UTC)


def _sensor_with_readings(
    store: SupabaseStore,
    service_role_client: Client,
    mac: str,
    readings: list[tuple[datetime, float, str]],
) -> str:
    device = store.insert_device(mac, name=f"Nodo {mac}")
    sensor_type = store.select_sensor_type("temperature", "degC")
    assert sensor_type is not None
    sensor = store.insert_sensor(device.id, sensor_type.id, "bmp280", "")
    assert sensor is not None
    service_role_client.table("measurements").insert(
        [
            {
                "sensor_id": sensor.id,
                "value": value,
                "timestamp": instant.isoformat(),
                "quality": quality,
            }
            for instant, value, quality in readings
        ]
    ).execute()
    return sensor.id


def _series(
    client: Client,
    sensor_id: str,
    start: datetime | None,
    end: datetime | None,
    bucket: str | None,
) -> list[dict[str, Any]]:
    params = {
        "p_sensor_id": sensor_id,
        "p_from": start.isoformat() if start else None,
        "p_to": end.isoformat() if end else None,
        "p_bucket": bucket,
    }
    rows: list[dict[str, Any]] = client.rpc("get_sensor_series", params).execute().data
    return rows


def _summary(rows: list[dict[str, Any]]) -> list[tuple[datetime, float, float, float, int]]:
    return [
        (
            datetime.fromisoformat(row["bucket"]),
            row["avg_value"],
            row["min_value"],
            row["max_value"],
            row["sample_count"],
        )
        for row in rows
    ]


def test_minute_buckets_aggregate_exactly(
    store: SupabaseStore,
    service_role_client: Client,
    authenticated_client: Client,
    unique_mac: str,
) -> None:
    readings = [
        (_START + timedelta(seconds=5), 10.0, "ok"),
        (_START + timedelta(seconds=20), 20.0, "ok"),
        (_START + timedelta(seconds=59), 30.0, "ok"),
        (_START + timedelta(minutes=1, seconds=1), 40.0, "ok"),
    ]
    sensor_id = _sensor_with_readings(store, service_role_client, unique_mac, readings)

    rows = _series(authenticated_client, sensor_id, _START, _START + timedelta(hours=1), "minute")

    assert _summary(rows) == [
        (_START, 20.0, 10.0, 30.0, 3),
        (_START + timedelta(minutes=1), 40.0, 40.0, 40.0, 1),
    ]


def test_hour_buckets_aggregate_exactly(
    store: SupabaseStore,
    service_role_client: Client,
    authenticated_client: Client,
    unique_mac: str,
) -> None:
    readings = [
        (_START + timedelta(minutes=15), 12.0, "ok"),
        (_START + timedelta(minutes=45), 18.0, "ok"),
        (_START + timedelta(hours=1, minutes=5), 7.5, "ok"),
    ]
    sensor_id = _sensor_with_readings(store, service_role_client, unique_mac, readings)

    rows = _series(authenticated_client, sensor_id, _START, _START + timedelta(days=1), "hour")

    assert _summary(rows) == [
        (_START, 15.0, 12.0, 18.0, 2),
        (_START + timedelta(hours=1), 7.5, 7.5, 7.5, 1),
    ]


def test_day_buckets_follow_the_argentine_calendar_day(
    store: SupabaseStore,
    service_role_client: Client,
    authenticated_client: Client,
    unique_mac: str,
) -> None:
    # 23:30 on Oct 2 and 01:00 and 02:00 on Oct 3 in Argentina (UTC-3).
    readings = [
        (datetime(2026, 10, 3, 2, 30, tzinfo=UTC), 5.0, "ok"),
        (datetime(2026, 10, 3, 4, 0, tzinfo=UTC), 6.0, "ok"),
        (datetime(2026, 10, 3, 5, 0, tzinfo=UTC), 8.0, "ok"),
    ]
    sensor_id = _sensor_with_readings(store, service_role_client, unique_mac, readings)

    rows = _series(
        authenticated_client,
        sensor_id,
        datetime(2026, 10, 1, tzinfo=UTC),
        datetime(2026, 10, 5, tzinfo=UTC),
        "day",
    )

    assert _summary(rows) == [
        (datetime(2026, 10, 2, 3, 0, tzinfo=UTC), 5.0, 5.0, 5.0, 1),
        (datetime(2026, 10, 3, 3, 0, tzinfo=UTC), 7.0, 6.0, 8.0, 2),
    ]


def test_only_ok_readings_inside_the_half_open_range_count(
    store: SupabaseStore,
    service_role_client: Client,
    authenticated_client: Client,
    unique_mac: str,
) -> None:
    end = _START + timedelta(minutes=10)
    readings = [
        (_START, 10.0, "ok"),
        (_START + timedelta(seconds=10), 900.0, "out_of_range"),
        (_START + timedelta(seconds=20), 500.0, "suspect"),
        (_START - timedelta(seconds=1), 99.0, "ok"),
        (end, 99.0, "ok"),
    ]
    sensor_id = _sensor_with_readings(store, service_role_client, unique_mac, readings)

    rows = _series(authenticated_client, sensor_id, _START, end, "minute")

    assert _summary(rows) == [(_START, 10.0, 10.0, 10.0, 1)]


def test_anon_cannot_execute_the_function(anon_client: Client) -> None:
    with pytest.raises(APIError) as error:
        _series(
            anon_client,
            "00000000-0000-0000-0000-000000000000",
            _START,
            _START + timedelta(hours=1),
            "minute",
        )

    assert error.value.code == _PERMISSION_DENIED


def test_execute_is_granted_to_authenticated_only(query_scalar: Callable[[str], str]) -> None:
    grants = query_scalar(
        "SELECT string_agg(r || '=' || has_function_privilege(r, "
        f"'{_SIGNATURE}', 'EXECUTE'), ',' ORDER BY r)"
        " FROM unnest(ARRAY['anon', 'authenticated']) AS r"
    )
    public_grant = query_scalar(
        "SELECT count(*) FROM pg_proc p,"
        " aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a"
        f" WHERE p.oid = '{_SIGNATURE}'::regprocedure AND a.grantee = 0"
    )

    assert grants == "anon=false,authenticated=true"
    assert public_grant == "0"


def test_function_is_stable_sql_invoker_with_empty_search_path(
    query_scalar: Callable[[str], str],
) -> None:
    attributes = query_scalar(
        "SELECT l.lanname || '|' || p.provolatile::text || '|' || p.prosecdef || '|' || "
        "array_to_string(p.proconfig, ',')"
        f" FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang"
        f" WHERE p.oid = '{_SIGNATURE}'::regprocedure"
    )

    assert attributes == 'sql|s|false|search_path=""'


@pytest.mark.parametrize("bucket", ["second", "week", "month", "MINUTE", "", None])
def test_bucket_outside_the_allowlist_is_rejected(
    authenticated_client: Client, bucket: str | None
) -> None:
    with pytest.raises(APIError) as error:
        _series(
            authenticated_client,
            "00000000-0000-0000-0000-000000000000",
            _START,
            _START + timedelta(hours=1),
            bucket,
        )

    assert "p_bucket must be minute, hour or day" in str(error.value.message)


@pytest.mark.parametrize("width", [timedelta(0), timedelta(hours=-1)])
def test_empty_or_inverted_range_is_rejected(
    authenticated_client: Client, width: timedelta
) -> None:
    with pytest.raises(APIError) as error:
        _series(
            authenticated_client,
            "00000000-0000-0000-0000-000000000000",
            _START,
            _START + width,
            "minute",
        )

    assert "p_to must be later than p_from" in str(error.value.message)


@pytest.mark.parametrize(
    ("start", "end"),
    [(None, _START + timedelta(hours=1)), (_START, None), (None, None)],
)
def test_missing_range_bound_is_rejected(
    authenticated_client: Client, start: datetime | None, end: datetime | None
) -> None:
    with pytest.raises(APIError) as error:
        _series(authenticated_client, "00000000-0000-0000-0000-000000000000", start, end, "minute")

    assert "p_to must be later than p_from" in str(error.value.message)


@pytest.mark.parametrize(
    ("bucket", "width"),
    [
        ("minute", timedelta(days=7, seconds=1)),
        ("hour", timedelta(days=400, seconds=1)),
        ("day", timedelta(days=3650, seconds=1)),
    ],
)
def test_range_wider_than_the_bucket_allows_is_rejected(
    authenticated_client: Client, bucket: str, width: timedelta
) -> None:
    with pytest.raises(APIError) as error:
        _series(
            authenticated_client,
            "00000000-0000-0000-0000-000000000000",
            _START,
            _START + width,
            bucket,
        )

    assert "range too wide for p_bucket" in str(error.value.message)


@pytest.mark.parametrize(
    ("bucket", "width"),
    [("minute", timedelta(days=7)), ("hour", timedelta(days=400)), ("day", timedelta(days=3650))],
)
def test_range_at_the_maximum_width_is_accepted(
    authenticated_client: Client, bucket: str, width: timedelta
) -> None:
    rows = _series(
        authenticated_client,
        "00000000-0000-0000-0000-000000000000",
        _START,
        _START + width,
        bucket,
    )

    assert rows == []


def test_rollback_drops_the_function_and_reapplying_restores_it(
    apply_sql: Callable[[Path], None], query_scalar: Callable[[str], str]
) -> None:
    exists = f"SELECT to_regprocedure('{_SIGNATURE}') IS NOT NULL"

    apply_sql(_SUPABASE_DIR / "rollbacks" / _SERIES)
    try:
        assert query_scalar(exists) == "f"
    finally:
        apply_sql(_SUPABASE_DIR / "migrations" / _SERIES)

    assert query_scalar(exists) == "t"
