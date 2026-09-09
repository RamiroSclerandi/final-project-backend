"""Behavioural tests for `Registry` against an in-memory fake store.

See docs/SDD_Worker_Ingesta.md section 5.3 for the resolution algorithm and
the spec requirement "Device and Sensor Auto-Registration" (CA-3, CA-4)
(sdd/worker-ingesta-mqtt/spec). supabase-py is a third party this project
does not own: `FakeRegistryStore` is a hand-written in-memory implementation
of the narrow `RegistryStore` port `Registry` depends on, never a mock of
supabase-py itself.
"""

from datetime import UTC, datetime
from uuid import uuid4

from ingest.domain.normalize import Reading
from ingest.registry import (
    DeviceRecord,
    Registry,
    SensorRecord,
    SensorTypeRecord,
)


def _make_reading(
    *,
    device_mac: str = "AABBCCDDEEFF",
    channel: str = "temperature",
    unit: str = "C",
    tag: str = "",
    source: str = "bmp280",
) -> Reading:
    return Reading(
        device_mac=device_mac,
        channel=channel,
        unit=unit,
        tag=tag,
        source=source,
        value=21.5,
        value_min=None,
        value_max=None,
        sample_count=None,
        recorded_at=datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC),
        ts_source="device",
        rssi=-60,
        seq=1,
        boot=1,
    )


class FakeRegistryStore:
    """In-memory `RegistryStore`.

    Tracks `query_count` across every select/insert call so tests can assert
    a cache hit issues no queries, without asserting a mock was called.
    `simulate_sensor_insert_race`, when set, makes the next `insert_sensor`
    behave like a real `ON CONFLICT DO NOTHING` that lost a race: another
    writer inserts the identical row first, and this call reports the
    conflict by returning `None` instead of raising.
    """

    def __init__(self) -> None:
        self.devices: dict[str, DeviceRecord] = {}
        self.sensor_types: dict[tuple[str, str], SensorTypeRecord] = {}
        self.sensors: dict[tuple[str, str, str, str], SensorRecord] = {}
        self.query_count = 0
        self.simulate_sensor_insert_race = False

    def select_device_by_mac(self, mac_address: str) -> DeviceRecord | None:
        self.query_count += 1
        return self.devices.get(mac_address)

    def insert_device(self, mac_address: str, name: str) -> DeviceRecord:
        self.query_count += 1
        record = DeviceRecord(id=str(uuid4()), mac_address=mac_address, name=name)
        self.devices[mac_address] = record
        return record

    def select_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        self.query_count += 1
        return self.sensor_types.get((name, unit))

    def insert_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        self.query_count += 1
        if (name, unit) in self.sensor_types:
            return None
        record = SensorTypeRecord(id=str(uuid4()), name=name, unit=unit)
        self.sensor_types[(name, unit)] = record
        return record

    def select_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        self.query_count += 1
        return self.sensors.get((device_id, type_id, source, tag))

    def insert_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        self.query_count += 1
        key = (device_id, type_id, source, tag)
        if self.simulate_sensor_insert_race:
            self.simulate_sensor_insert_race = False
            self.sensors[key] = SensorRecord(
                id=str(uuid4()), device_id=device_id, type_id=type_id, source=source, tag=tag
            )
            return None
        if key in self.sensors:
            return None
        record = SensorRecord(
            id=str(uuid4()), device_id=device_id, type_id=type_id, source=source, tag=tag
        )
        self.sensors[key] = record
        return record


def test_known_sensor_resolves_from_cache_without_touching_the_store() -> None:
    store = FakeRegistryStore()
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading()
    first_id = registry.resolve(reading)
    store.query_count = 0

    second_id = registry.resolve(reading)

    assert second_id == first_id
    assert store.query_count == 0


def test_unknown_device_is_auto_registered() -> None:
    store = FakeRegistryStore()
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading(device_mac="AABBCCDDEEFF")

    registry.resolve(reading)

    device = store.devices["AABBCCDDEEFF"]
    assert device.mac_address == "AABBCCDDEEFF"
    assert device.name == "Nodo AABBCCDDEEFF"


def test_unknown_channel_on_known_device_is_auto_registered() -> None:
    store = FakeRegistryStore()
    existing_device = DeviceRecord(
        id="device-1", mac_address="AABBCCDDEEFF", name="Nodo AABBCCDDEEFF"
    )
    store.devices["AABBCCDDEEFF"] = existing_device
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading(device_mac="AABBCCDDEEFF", channel="humidity", unit="%", source="dht22")

    sensor_id = registry.resolve(reading)

    sensor_type = store.sensor_types[("humidity", "%")]
    sensor = store.sensors[(existing_device.id, sensor_type.id, "dht22", "")]
    assert sensor.id == sensor_id


def test_repeated_resolution_issues_no_further_queries() -> None:
    store = FakeRegistryStore()
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading()

    first_id = registry.resolve(reading)
    queries_after_first = store.query_count
    second_id = registry.resolve(reading)
    third_id = registry.resolve(reading)

    assert first_id == second_id == third_id
    assert store.query_count == queries_after_first


def test_cache_expires_after_the_ttl() -> None:
    store = FakeRegistryStore()
    current_time = [0.0]
    registry = Registry(store, ttl_seconds=10, clock=lambda: current_time[0])
    reading = _make_reading()

    registry.resolve(reading)
    queries_after_first = store.query_count
    current_time[0] += 11
    registry.resolve(reading)

    assert store.query_count > queries_after_first


def test_conflicting_insert_that_returns_no_row_is_recovered_by_reselecting() -> None:
    store = FakeRegistryStore()
    store.simulate_sensor_insert_race = True
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading()

    sensor_id = registry.resolve(reading)

    key = next(iter(store.sensors))
    assert store.sensors[key].id == sensor_id


def test_expected_range_returns_thresholds_from_the_sensor_type() -> None:
    store = FakeRegistryStore()
    store.sensor_types[("temperature", "C")] = SensorTypeRecord(
        id="type-1", name="temperature", unit="C", expected_min=-40.0, expected_max=85.0
    )
    registry = Registry(store, ttl_seconds=900)

    expected_min, expected_max = registry.expected_range("temperature", "C")

    assert (expected_min, expected_max) == (-40.0, 85.0)


def test_expected_range_is_none_for_a_sensor_type_with_no_configured_thresholds() -> None:
    store = FakeRegistryStore()
    registry = Registry(store, ttl_seconds=900)
    reading = _make_reading()
    registry.resolve(reading)  # auto-registers "temperature"/"C" with no thresholds

    expected_min, expected_max = registry.expected_range("temperature", "C")

    assert (expected_min, expected_max) == (None, None)


def test_expected_range_is_cached_and_issues_no_further_queries_within_the_ttl() -> None:
    store = FakeRegistryStore()
    store.sensor_types[("temperature", "C")] = SensorTypeRecord(
        id="type-1", name="temperature", unit="C", expected_min=-40.0, expected_max=85.0
    )
    registry = Registry(store, ttl_seconds=900)
    registry.expected_range("temperature", "C")
    queries_after_first = store.query_count

    registry.expected_range("temperature", "C")

    assert store.query_count == queries_after_first
