"""Node-health read paths (ported from the frontend node-health suite).

`raw_messages` is diagnostic material with RLS enabled and no policy, so no
user role may read it. A sensor with no measurements is invisible in
`v_latest_readings` until its first measurement lands, which is how the
dashboard detects a new sensor.
"""

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_FIRST_VALUE = 33.3


def test_raw_messages_are_hidden_from_authenticated_and_anon(
    service_role_client: Client, authenticated_client: Client, anon_client: Client, unique_mac: str
) -> None:
    seeded = {"topic": f"dl/v1/{unique_mac}/data", "payload": {"value": 1}, "source": "hivemq"}
    row_id = service_role_client.table("raw_messages").insert(seeded).execute().data[0]["id"]

    try:
        authenticated_rows = (
            authenticated_client.table("raw_messages").select("id").eq("id", row_id).execute().data
        )
        try:
            anon_rows = (
                anon_client.table("raw_messages").select("id").eq("id", row_id).execute().data
            )
        except APIError:
            anon_rows = []

        assert authenticated_rows == []
        assert anon_rows == []
    finally:
        service_role_client.table("raw_messages").delete().eq("id", row_id).execute()


def test_new_sensor_appears_in_latest_readings_after_its_first_measurement(
    service_role_client: Client, authenticated_client: Client, unique_mac: str
) -> None:
    device = {"mac_address": unique_mac, "name": f"Nodo {unique_mac}"}
    device_id = service_role_client.table("devices").insert(device).execute().data[0]["id"]
    sensor_type = (
        service_role_client.table("sensor_types").select("id").eq("name", "temperature").execute()
    )
    sensor = {"device_id": device_id, "type_id": sensor_type.data[0]["id"], "source": "health-test"}

    try:
        sensor_id = service_role_client.table("sensors").insert(sensor).execute().data[0]["id"]
        before = (
            authenticated_client.table("v_latest_readings")
            .select("sensor_id")
            .eq("sensor_id", sensor_id)
            .execute()
            .data
        )

        service_role_client.table("measurements").insert(
            {"sensor_id": sensor_id, "value": _FIRST_VALUE, "timestamp": "2026-01-01T00:00:00Z"}
        ).execute()
        after = (
            authenticated_client.table("v_latest_readings")
            .select("sensor_id, value")
            .eq("sensor_id", sensor_id)
            .execute()
            .data
        )

        assert before == []
        assert after == [{"sensor_id": sensor_id, "value": _FIRST_VALUE}]
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()
