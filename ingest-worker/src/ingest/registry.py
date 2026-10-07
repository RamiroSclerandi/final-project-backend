"""Resolve readings to `sensor_id`, registering unseen devices, sensor types and sensors."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ingest.domain.normalize import Reading

_CacheKey = tuple[str, str, str, str, str]


@dataclass(frozen=True)
class DeviceRecord:
    """One row of `devices`, narrowed to what resolution needs."""

    id: str
    mac_address: str
    name: str


@dataclass(frozen=True)
class SensorTypeRecord:
    """One `sensor_types` row; thresholds are editable, so a cached copy can lag one TTL."""

    id: str
    name: str
    unit: str
    expected_min: float | None = None
    expected_max: float | None = None


@dataclass(frozen=True)
class SensorRecord:
    """One row of `sensors`, narrowed to what resolution needs."""

    id: str
    device_id: str
    type_id: str
    source: str
    tag: str


class RegistryStore(Protocol):
    """Persistence port the registry needs.

    `insert_sensor_type`/`insert_sensor` behave like INSERT ... ON CONFLICT DO NOTHING:
    `None` means another writer won the race, and callers must re-select.
    """

    def select_device_by_mac(self, mac_address: str) -> DeviceRecord | None: ...

    def insert_device(self, mac_address: str, name: str) -> DeviceRecord: ...

    def select_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None: ...

    def insert_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None: ...

    def select_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None: ...

    def insert_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None: ...


class RegistryResolutionError(RuntimeError):
    """A row vanished between an insert conflict and the re-select."""


class Registry:
    """Resolves `Reading`s to `sensor_id` through TTL caches. Single writer thread only.

    Ids are immutable, so a cached id is trusted for one TTL; a row deleted from the
    web app is noticed within that window. Sensor type thresholds are editable, so
    quality banding can lag an edit by one TTL.
    """

    def __init__(
        self,
        store: RegistryStore,
        ttl_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._cache: dict[_CacheKey, tuple[str, float]] = {}
        self._sensor_type_cache: dict[tuple[str, str], tuple[SensorTypeRecord, float]] = {}

    def resolve(self, reading: Reading) -> str:
        """Resolve one reading to its `sensor_id`, registering rows as needed."""
        key = self._cache_key(reading)
        cached_id = self._cached(key)
        if cached_id is not None:
            return cached_id

        device = self._resolve_device(reading.device_mac)
        sensor_type = self._resolve_sensor_type(reading.channel, reading.unit)
        sensor = self._resolve_sensor(device.id, sensor_type.id, reading.source, reading.tag)

        self._cache[key] = (sensor.id, self._clock())
        return sensor.id

    def expected_range(self, channel: str, unit: str) -> tuple[float | None, float | None]:
        """Return `(expected_min, expected_max)` for a channel, from the sensor type cache."""
        sensor_type = self._resolve_sensor_type(channel, unit)
        return (sensor_type.expected_min, sensor_type.expected_max)

    def ensure_device(self, mac_address: str) -> str:
        """Resolve a device by MAC, registering it if unseen.

        A retained status arrives before any data message, and an UPDATE on a missing
        row is silently lost.
        """
        return self._resolve_device(mac_address).id

    def _cached(self, key: _CacheKey) -> str | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        sensor_id, cached_at = entry
        if self._clock() - cached_at >= self._ttl_seconds:
            del self._cache[key]
            return None
        return sensor_id

    def _resolve_device(self, mac_address: str) -> DeviceRecord:
        device = self._store.select_device_by_mac(mac_address)
        if device is not None:
            return device
        return self._store.insert_device(mac_address, name=f"Nodo {mac_address}")

    def _resolve_sensor_type(self, name: str, unit: str) -> SensorTypeRecord:
        key = (name, unit)
        cached = self._sensor_type_cache.get(key)
        if cached is not None:
            record, cached_at = cached
            if self._clock() - cached_at < self._ttl_seconds:
                return record
            del self._sensor_type_cache[key]

        record = self._fetch_sensor_type(name, unit)
        self._sensor_type_cache[key] = (record, self._clock())
        return record

    def _fetch_sensor_type(self, name: str, unit: str) -> SensorTypeRecord:
        existing = self._store.select_sensor_type(name, unit)
        if existing is not None:
            return existing
        inserted = self._store.insert_sensor_type(name, unit)
        if inserted is not None:
            return inserted
        recovered = self._store.select_sensor_type(name, unit)
        if recovered is None:
            raise RegistryResolutionError(
                f"sensor_type (name={name!r}, unit={unit!r}) insert reported a "
                "conflict but no row exists on re-select"
            )
        return recovered

    def _resolve_sensor(self, device_id: str, type_id: str, source: str, tag: str) -> SensorRecord:
        existing = self._store.select_sensor(device_id, type_id, source, tag)
        if existing is not None:
            return existing
        inserted = self._store.insert_sensor(device_id, type_id, source, tag)
        if inserted is not None:
            return inserted
        recovered = self._store.select_sensor(device_id, type_id, source, tag)
        if recovered is None:
            raise RegistryResolutionError(
                f"sensor (device_id={device_id!r}, type_id={type_id!r}, "
                f"source={source!r}, tag={tag!r}) insert reported a conflict "
                "but no row exists on re-select"
            )
        return recovered

    @staticmethod
    def _cache_key(reading: Reading) -> _CacheKey:
        return (reading.device_mac, reading.channel, reading.unit, reading.tag, reading.source)
