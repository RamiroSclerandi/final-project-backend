"""Firmware loss counters on `measurements`: `lost` and `store_drop`, and their rollback."""

import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from postgrest.exceptions import APIError
from supabase import Client

from ingest.sink.supabase_sink import SupabaseStore

pytestmark = pytest.mark.integration

_SUPABASE_DIR = Path(__file__).resolve().parents[3] / "supabase"
_LOSS_COUNTERS = "20261002140000_measurements_loss_counters.sql"
_SCHEMA_RELOAD_TIMEOUT_S = 10.0


def _sensor_id(store: SupabaseStore, mac: str) -> str:
    device = store.insert_device(mac, name=f"Nodo {mac}")
    sensor_type = store.select_sensor_type("temperature", "degC")
    assert sensor_type is not None
    sensor = store.insert_sensor(device.id, sensor_type.id, "bmp280", "")
    assert sensor is not None
    return sensor.id


def _wait_for_postgrest_to_see_loss_counters(client: Client) -> None:
    deadline = time.monotonic() + _SCHEMA_RELOAD_TIMEOUT_S
    while True:
        try:
            client.table("measurements").select("lost,store_drop").limit(1).execute()
            return
        except APIError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)


def test_loss_counters_round_trip_through_the_worker_upsert(
    store: SupabaseStore, service_role_client: Client, unique_mac: str
) -> None:
    sensor_id = _sensor_id(store, unique_mac)
    row = {
        "sensor_id": sensor_id,
        "value": 21.5,
        "timestamp": datetime(2026, 10, 2, 12, 0, tzinfo=UTC).isoformat(),
        "ts_source": "device",
        "quality": "ok",
        "rssi": -60,
        "seq": 7,
        "boot": 2,
        "value_min": None,
        "value_max": None,
        "sample_count": None,
        "lost": 3,
        "store_drop": None,
    }

    store.upsert_measurements([row])

    stored = (
        service_role_client.table("measurements")
        .select("lost,store_drop")
        .eq("sensor_id", sensor_id)
        .execute()
        .data
    )
    assert stored == [{"lost": 3, "store_drop": None}]


def test_negative_loss_counters_are_rejected(
    store: SupabaseStore, service_role_client: Client, unique_mac: str
) -> None:
    sensor_id = _sensor_id(store, unique_mac)

    with pytest.raises(APIError, match="23514"):
        service_role_client.table("measurements").insert(
            {
                "sensor_id": sensor_id,
                "value": 1.0,
                "timestamp": datetime(2026, 10, 2, 12, 0, tzinfo=UTC).isoformat(),
                "store_drop": -1,
            }
        ).execute()


def test_rollback_drops_the_loss_counters_and_reapplying_restores_them(
    apply_sql: Callable[[Path], None],
    query_scalar: Callable[[str], str],
    service_role_client: Client,
) -> None:
    columns = (
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name = 'measurements' AND column_name IN ('lost', 'store_drop')"
    )

    apply_sql(_SUPABASE_DIR / "rollbacks" / _LOSS_COUNTERS)
    try:
        assert query_scalar(columns) == "0"
    finally:
        apply_sql(_SUPABASE_DIR / "migrations" / _LOSS_COUNTERS)
        _wait_for_postgrest_to_see_loss_counters(service_role_client)

    assert query_scalar(columns) == "2"


_ATTRIBUTION_QUERY = (
    Path(__file__).resolve().parents[2] / "docs" / "queries" / "loss_attribution.sql"
)
_ATTRIBUTION_MAC = "10550A770001"


def test_loss_attribution_splits_a_seq_gap_into_buffer_drops_and_transport_loss(
    store: SupabaseStore,
    service_role_client: Client,
    query_scalar: Callable[[str], str],
) -> None:
    temperature = _sensor_id(store, _ATTRIBUTION_MAC)
    pressure_type = store.select_sensor_type("pressure", "hPa")
    assert pressure_type is not None
    device = store.select_device_by_mac(_ATTRIBUTION_MAC)
    assert device is not None
    pressure = store.insert_sensor(device.id, pressure_type.id, "bmp280", "")
    assert pressure is not None
    # Boot 1 sends seq 1, 2 and 5: seq 3 and 4 are missing, and the buffer
    # reports one drop between seq 2 and 5, so one message died in transport.
    rows = [
        {
            "sensor_id": sensor_id,
            "value": 1.0,
            "timestamp": datetime(2026, 10, 2, 12, seq, tzinfo=UTC).isoformat(),
            "seq": seq,
            "boot": 1,
            "lost": lost,
            "store_drop": store_drop,
        }
        for sensor_id in (temperature, pressure.id)
        for seq, lost, store_drop in ((1, 0, 0), (2, 1, 0), (5, 2, 1))
    ]
    service_role_client.table("measurements").insert(rows).execute()
    attribution_sql = _ATTRIBUTION_QUERY.read_text().strip().rstrip(";")

    result = query_scalar(
        "SELECT total_seq_gap || '|' || delta_store_drop || '|' || true_transport_loss"
        f" || '|' || delta_lost FROM ({attribution_sql}) attribution"
        f" WHERE mac_address = '{_ATTRIBUTION_MAC}'"
    )

    assert result == "2|1|1|2"
