"""Read access by role on `devices`, `sensors` and `measurements`.

`authenticated` reads all three; `anon` reads none of them, and no user role
can insert a measurement: only the worker's `service_role` writes. Each test
seeds a real row first, because an empty result proves nothing unless a row
existed to leak.
"""

from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_PERMISSION_DENIED = "42501"
_SEEDED_VALUE = 21.5


@dataclass(frozen=True)
class SeededRows:
    device_id: str
    sensor_id: str


@pytest.fixture
def seeded_rows(service_role_client: Client, unique_mac: str) -> Iterator[SeededRows]:
    """Seed one device, sensor and measurement; the device delete cascades to the rest."""
    device = {"mac_address": unique_mac, "name": f"Nodo {unique_mac}"}
    device_id = str(service_role_client.table("devices").insert(device).execute().data[0]["id"])

    try:
        sensor_type = (
            service_role_client.table("sensor_types")
            .select("id")
            .eq("name", "temperature")
            .execute()
        )
        sensor = {
            "device_id": device_id,
            "type_id": sensor_type.data[0]["id"],
            "source": "reads-test",
        }
        sensor_id = str(service_role_client.table("sensors").insert(sensor).execute().data[0]["id"])
        service_role_client.table("measurements").insert(
            {"sensor_id": sensor_id, "value": _SEEDED_VALUE, "timestamp": "2026-01-01T00:00:00Z"}
        ).execute()

        yield SeededRows(device_id=device_id, sensor_id=sensor_id)
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()


def _anon_read_ids(anon_client: Client, table: str, column: str, value: str) -> list[object]:
    """Read `column` filtered to the seeded row.

    `anon` holds SELECT but RLS filters every row, so PostgREST returns an empty
    list; any API error (a typo, a missing grant) must surface, not read as "no rows".
    """
    return anon_client.table(table).select(column).eq(column, value).execute().data


def test_authenticated_reads_the_seeded_device(
    authenticated_client: Client, seeded_rows: SeededRows
) -> None:
    rows = (
        authenticated_client.table("devices")
        .select("id")
        .eq("id", seeded_rows.device_id)
        .execute()
        .data
    )

    assert rows == [{"id": seeded_rows.device_id}]


def test_authenticated_reads_the_seeded_sensor(
    authenticated_client: Client, seeded_rows: SeededRows
) -> None:
    rows = (
        authenticated_client.table("sensors")
        .select("id")
        .eq("id", seeded_rows.sensor_id)
        .execute()
        .data
    )

    assert rows == [{"id": seeded_rows.sensor_id}]


def test_authenticated_reads_the_seeded_measurement(
    authenticated_client: Client, seeded_rows: SeededRows
) -> None:
    rows = (
        authenticated_client.table("measurements")
        .select("sensor_id, value")
        .eq("sensor_id", seeded_rows.sensor_id)
        .execute()
        .data
    )

    assert rows == [{"sensor_id": seeded_rows.sensor_id, "value": _SEEDED_VALUE}]


def test_anon_sees_no_seeded_device(anon_client: Client, seeded_rows: SeededRows) -> None:
    assert _anon_read_ids(anon_client, "devices", "id", seeded_rows.device_id) == []


def test_anon_sees_no_seeded_sensor(anon_client: Client, seeded_rows: SeededRows) -> None:
    assert _anon_read_ids(anon_client, "sensors", "id", seeded_rows.sensor_id) == []


def test_anon_sees_no_seeded_measurement(anon_client: Client, seeded_rows: SeededRows) -> None:
    assert _anon_read_ids(anon_client, "measurements", "sensor_id", seeded_rows.sensor_id) == []


def test_authenticated_cannot_insert_a_measurement(
    service_role_client: Client, authenticated_client: Client, seeded_rows: SeededRows
) -> None:
    with pytest.raises(APIError) as exc_info:
        authenticated_client.table("measurements").insert(
            {"sensor_id": seeded_rows.sensor_id, "value": 99, "timestamp": "2026-01-02T00:00:00Z"}
        ).execute()

    stored = (
        service_role_client.table("measurements")
        .select("value")
        .eq("sensor_id", seeded_rows.sensor_id)
        .execute()
        .data
    )
    assert exc_info.value.code == _PERMISSION_DENIED
    assert stored == [{"value": _SEEDED_VALUE}]
