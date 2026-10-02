"""`device_configs` is read-only to users; only the Edge Function (service role) writes."""

from collections.abc import Callable
from pathlib import Path

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_SUPABASE_DIR = Path(__file__).resolve().parents[3] / "supabase"
_MIGRATION = _SUPABASE_DIR / "migrations" / "20260930120000_restrict_device_configs_writes.sql"
_ROLLBACK = _SUPABASE_DIR / "rollbacks" / "20260930120000_restrict_device_configs_writes.sql"


def _device_id(service_role_client: Client, mac: str) -> str:
    device = {"mac_address": mac, "name": f"Nodo {mac}"}
    row = service_role_client.table("devices").insert(device).execute().data[0]
    return str(row["id"])


def _write_config(client: Client, device_id: str) -> None:
    client.table("device_configs").upsert(
        {"device_id": device_id, "sampling_interval_ms": 10_000}
    ).execute()


def test_authenticated_users_read_but_cannot_write_device_configs(
    service_role_client: Client, authenticated_client: Client, unique_mac: str
) -> None:
    device_id = _device_id(service_role_client, unique_mac)
    _write_config(service_role_client, device_id)

    rows = (
        authenticated_client.table("device_configs")
        .select("device_id")
        .eq("device_id", device_id)
        .execute()
        .data
    )

    assert rows == [{"device_id": device_id}]
    with pytest.raises(APIError, match="42501"):
        _write_config(authenticated_client, device_id)


def test_rollback_restores_user_writes_and_reapplying_removes_them(
    service_role_client: Client,
    authenticated_client: Client,
    apply_sql: Callable[[Path], None],
    unique_mac: str,
) -> None:
    device_id = _device_id(service_role_client, unique_mac)

    apply_sql(_ROLLBACK)
    try:
        _write_config(authenticated_client, device_id)
    finally:
        apply_sql(_MIGRATION)

    with pytest.raises(APIError, match="42501"):
        _write_config(authenticated_client, device_id)
