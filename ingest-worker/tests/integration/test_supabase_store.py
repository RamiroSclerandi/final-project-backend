"""`SupabaseStore` against a real, ephemeral Postgres + PostgREST pair.

Two of the three production defects this repository shipped are covered
here (see the module docstring in tests/integration/conftest.py):

1. `raw_messages.source` is NOT NULL; the in-memory fakes in
   tests/sink/test_supabase_sink.py accept any dictionary, so only a real
   PostgREST insert can fail the way production did.
2. `sensors.label` is a real column `SensorRecord` never declares. A regression
   to splatting the full PostgREST row into the dataclass fails here with a
   `TypeError` from an unexpected keyword argument -- exactly the production
   failure -- instead of silently passing against a fake.

The batch-upsert tests turn spike S1's measured PostgREST behavior
(`on_conflict=...,ignore_duplicates=True` is partial, not atomic) into a
regression guard.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from supabase import Client

from ingest.domain.normalize import Reading
from ingest.registry import Registry
from ingest.sink.supabase_sink import SupabaseStore

pytestmark = pytest.mark.integration


def _measurement_row(sensor_id: str, *, timestamp: datetime, value: float) -> dict[str, Any]:
    """Build one `measurements` row shaped like `MeasurementSink._to_measurement_row`."""
    return {
        "sensor_id": sensor_id,
        "value": value,
        "timestamp": timestamp.isoformat(),
        "ts_source": "device",
        "quality": "ok",
        "rssi": -60,
        "seq": 1,
        "boot": 1,
        "value_min": None,
        "value_max": None,
        "sample_count": None,
    }


def _reading(mac: str, *, value: float = 21.5) -> Reading:
    return Reading(
        device_mac=mac,
        channel="temperature",
        unit="degC",
        tag="",
        source="bmp280",
        value=value,
        value_min=None,
        value_max=None,
        sample_count=None,
        recorded_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        ts_source="device",
        rssi=-60,
        seq=1,
        boot=1,
    )


@pytest.fixture
def registered_sensor_id(store: SupabaseStore, unique_mac: str) -> str:
    """A sensor_id resolved through the real Registry/SupabaseStore, for measurement tests."""
    registry = Registry(store, ttl_seconds=900)
    return registry.resolve(_reading(unique_mac))


def test_archiving_a_message_writes_a_raw_messages_row(
    store: SupabaseStore, service_role_client: Client, unique_mac: str
) -> None:
    topic = f"dl/v1/{unique_mac}/data"
    received_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)

    store.archive_raw_message(topic=topic, payload=b'{"v": 1}', received_at=received_at, error=None)

    rows = (
        service_role_client.table("raw_messages")
        .select("topic,source,processed,error")
        .eq("topic", topic)
        .execute()
        .data
    )
    assert len(rows) == 1
    assert rows[0]["source"] == "hivemq"
    assert rows[0]["processed"] is True
    assert rows[0]["error"] is None


def test_archiving_an_unparseable_message_writes_its_error_and_leaves_it_unprocessed(
    store: SupabaseStore, service_role_client: Client, unique_mac: str
) -> None:
    topic = f"dl/v1/{unique_mac}/data"

    store.archive_raw_message(
        topic=topic,
        payload=b"not json",
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error="boom",
    )

    rows = (
        service_role_client.table("raw_messages")
        .select("processed,error")
        .eq("topic", topic)
        .execute()
        .data
    )
    assert len(rows) == 1
    assert rows[0]["processed"] is False
    assert rows[0]["error"] == "boom"


def test_registering_a_device_and_a_sensor_round_trips_through_insert_and_reselect(
    store: SupabaseStore, unique_mac: str
) -> None:
    registry = Registry(store, ttl_seconds=900)

    sensor_id = registry.resolve(_reading(unique_mac))

    device = store.select_device_by_mac(unique_mac)
    assert device is not None
    sensor_type = store.select_sensor_type("temperature", "degC")
    assert sensor_type is not None
    reselected = store.select_sensor(device.id, sensor_type.id, "bmp280", "")
    assert reselected is not None
    assert reselected.id == sensor_id


def test_a_batch_upsert_with_one_duplicate_writes_new_rows_and_skips_the_duplicate(
    store: SupabaseStore, service_role_client: Client, registered_sensor_id: str
) -> None:
    ts = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    store.upsert_measurements([_measurement_row(registered_sensor_id, timestamp=ts, value=21.0)])

    duplicate_row = _measurement_row(registered_sensor_id, timestamp=ts, value=999.0)
    new_row = _measurement_row(
        registered_sensor_id, timestamp=ts + timedelta(seconds=15), value=22.0
    )

    written = store.upsert_measurements([duplicate_row, new_row])

    assert written == 1
    rows = (
        service_role_client.table("measurements")
        .select("value")
        .eq("sensor_id", registered_sensor_id)
        .execute()
        .data
    )
    values = sorted(row["value"] for row in rows)
    assert values == [21.0, 22.0]


def test_replaying_an_identical_batch_changes_no_row_count(
    store: SupabaseStore, service_role_client: Client, registered_sensor_id: str
) -> None:
    ts = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    rows = [
        _measurement_row(registered_sensor_id, timestamp=ts, value=21.0),
        _measurement_row(registered_sensor_id, timestamp=ts + timedelta(seconds=15), value=22.0),
    ]

    first_written = store.upsert_measurements(rows)
    second_written = store.upsert_measurements(rows)

    assert first_written == 2
    assert second_written == 0
    stored_rows = (
        service_role_client.table("measurements")
        .select("id")
        .eq("sensor_id", registered_sensor_id)
        .execute()
        .data
    )
    assert len(stored_rows) == 2
