"""`MeasurementSink` against a real, ephemeral Postgres + PostgREST pair.

Covers the third production defect (see tests/integration/conftest.py's
module docstring): a retained device status arrives the moment the worker
subscribes, before any data message has registered the device. An UPDATE
matching zero rows returns 200 from PostgREST, so the status was silently
lost against the fakes -- only a real PostgREST UPDATE demonstrates the
zero-rows-matched behavior `Registry.ensure_device` exists to avoid.
"""

from datetime import UTC, datetime

import pytest
from supabase import Client

from ingest.registry import Registry
from ingest.sink.supabase_sink import MeasurementSink, SupabaseStore
from ingest.sources.base import DeviceStatus

pytestmark = pytest.mark.integration


def test_a_retained_status_for_an_unregistered_device_registers_it_and_sets_its_status(
    store: SupabaseStore, service_role_client: Client, unique_mac: str
) -> None:
    registry = Registry(store, ttl_seconds=900)
    sink = MeasurementSink(
        store=store, registry=registry, batch_max_size=100, batch_max_age_ms=2000
    )

    sink.handle_status(
        DeviceStatus(
            device_mac=unique_mac, online=True, received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
        )
    )

    rows = (
        service_role_client.table("devices")
        .select("mac_address,status")
        .eq("mac_address", unique_mac)
        .execute()
        .data
    )
    assert len(rows) == 1
    assert rows[0]["status"] is True
