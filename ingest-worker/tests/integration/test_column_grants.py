"""Column-scoped UPDATE grants on `devices` and `sensors` (ported from the frontend suite).

RLS decides which rows an `authenticated` user can reach, not which columns;
the column restriction is a GRANT. These tests prove the grant surface lets
the UI rename and relocate a device or relabel a sensor, while identity
columns such as `mac_address` stay out of reach.
"""

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_PERMISSION_DENIED = "42501"


def _seed_device(service_role_client: Client, mac: str) -> str:
    device = {"mac_address": mac, "name": "Original name"}
    row = service_role_client.table("devices").insert(device).execute().data[0]
    return str(row["id"])


def _seed_sensor(service_role_client: Client, device_id: str) -> str:
    sensor_type = (
        service_role_client.table("sensor_types").select("id").eq("name", "temperature").execute()
    )
    sensor = {
        "device_id": device_id,
        "type_id": sensor_type.data[0]["id"],
        "source": "grants-test",
        "label": "Original label",
    }
    row = service_role_client.table("sensors").insert(sensor).execute().data[0]
    return str(row["id"])


def test_authenticated_user_renames_and_relocates_a_device(
    service_role_client: Client, authenticated_client: Client, unique_mac: str
) -> None:
    device_id = _seed_device(service_role_client, unique_mac)

    try:
        authenticated_client.table("devices").update(
            {"name": "Renamed device", "location_ref": "Server room"}
        ).eq("id", device_id).execute()

        row = (
            service_role_client.table("devices")
            .select("name, location_ref")
            .eq("id", device_id)
            .single()
            .execute()
            .data
        )
        assert row == {"name": "Renamed device", "location_ref": "Server room"}
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()


def test_authenticated_user_relabels_a_sensor(
    service_role_client: Client, authenticated_client: Client, unique_mac: str
) -> None:
    device_id = _seed_device(service_role_client, unique_mac)
    sensor_id = _seed_sensor(service_role_client, device_id)

    try:
        authenticated_client.table("sensors").update({"label": "Renamed label"}).eq(
            "id", sensor_id
        ).execute()

        row = (
            service_role_client.table("sensors")
            .select("label")
            .eq("id", sensor_id)
            .single()
            .execute()
            .data
        )
        assert row == {"label": "Renamed label"}
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()


def test_authenticated_update_including_mac_address_is_rejected(
    service_role_client: Client, authenticated_client: Client, unique_mac: str
) -> None:
    device_id = _seed_device(service_role_client, unique_mac)

    try:
        with pytest.raises(APIError) as exc_info:
            authenticated_client.table("devices").update(
                {"name": "Attempted rename", "mac_address": "BADBADBADBAD"}
            ).eq("id", device_id).execute()

        row = (
            service_role_client.table("devices")
            .select("mac_address, name")
            .eq("id", device_id)
            .single()
            .execute()
            .data
        )
        assert exc_info.value.code == _PERMISSION_DENIED
        assert row == {"mac_address": unique_mac, "name": "Original name"}
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()


def test_anon_update_leaves_the_device_unchanged(
    service_role_client: Client, anon_client: Client, unique_mac: str
) -> None:
    device_id = _seed_device(service_role_client, unique_mac)

    try:
        # anon has no UPDATE grant at all: it either errors or matches zero rows.
        try:
            anon_client.table("devices").update({"name": "Anon rename"}).eq(
                "id", device_id
            ).execute()
        except APIError:
            pass

        row = (
            service_role_client.table("devices")
            .select("name")
            .eq("id", device_id)
            .single()
            .execute()
            .data
        )
        assert row == {"name": "Original name"}
    finally:
        service_role_client.table("devices").delete().eq("id", device_id).execute()
