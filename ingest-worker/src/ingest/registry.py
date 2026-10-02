"""Resolve a `Reading` to its `sensor_id`, auto-registering unseen devices,
sensor types, and sensors, backed by a bounded-TTL cache.

This is what lets a new node or a new channel register itself without a code
deploy.
"""

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
    """One row of `sensor_types`, narrowed to what resolution needs.

    `expected_min`/`expected_max` are the quality-banding thresholds the
    sink compares each reading against. They
    are editable through the web platform, so a cached record can lag a
    threshold edit by up to one TTL — see `Registry.expected_range`.
    """

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
    """Persistence port the registry needs, independent of any DB client.

    Every `insert_sensor_type`/`insert_sensor` call mirrors
    `INSERT ... ON CONFLICT DO NOTHING`: it returns `None`, not an error,
    when a concurrent writer (another worker instance, or the web platform)
    already inserted the identical row between the caller's SELECT and this
    INSERT. Callers MUST re-select on `None`, never treat it as failure.
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
    """A row vanished between an insert conflict and the recovery re-select.

    Distinct from the expected race (insert conflict, then a successful
    re-select): this means the store itself returned an inconsistent result,
    not merely that another writer won a race.
    """


class Registry:
    """Resolves `Reading`s to `sensor_id`, auto-registering unseen rows.

    Resolution is a cache hit, else
    devices -> sensor_types -> sensors (SELECT, INSERT-ON-CONFLICT-DO-NOTHING,
    SELECT again), then cache and return.

    Threading contract: an instance is confined to the single writer thread
    that owns the pending batch and the Supabase client. It takes no
    lock and is not safe for concurrent use from more than one thread.

    Cache scope: the resolved `sensor_id` is cached, keyed by
    `(mac, channel, unit, tag, source)`; `devices.name` and every other
    mutable column besides sensor-type thresholds are never cached, because
    the web platform's UI can edit those columns concurrently while ids are
    immutable by construction. A cached id is trusted for `ttl_seconds`
    (`REGISTRY_CACHE_TTL_S`); after that it expires and resolution runs
    again. This bounds the one real staleness risk of caching an id at all —
    that the row was deleted through the web platform in the meantime — to
    at most one TTL window.

    A second, separate TTL-bound cache holds the resolved `SensorTypeRecord`
    (including `expected_min`/`expected_max`) keyed by `(name, unit)`. Its
    thresholds ARE mutable through the web platform, so quality banding
    (`expected_range`) can be stale for up to one TTL window after a
    threshold edit. Accepted: ids themselves cannot go stale this way.
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
        """Resolve one `Reading` to its `sensor_id`, registering as needed.

        Args:
            reading: One normalized channel reading.

        Returns:
            The id of the `sensors` row for this device/channel/source/tag.
        """
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
        """Return the accepted `[expected_min, expected_max]` range for a channel/unit.

        Used by the sink for quality banding.
        Backed by the same TTL-bound `sensor_types` cache `resolve()`
        populates, so calling this after `resolve()` for the same channel
        issues no further query.

        Args:
            channel: `sensor_types.name` — same value as `Reading.channel`.
            unit: `sensor_types.unit`.

        Returns:
            `(expected_min, expected_max)`, either or both `None` when the
            sensor type has no configured threshold.
        """
        sensor_type = self._resolve_sensor_type(channel, unit)
        return (sensor_type.expected_min, sensor_type.expected_max)

    def ensure_device(self, mac_address: str) -> str:
        """Resolve a device by MAC, registering it if unseen.

        A retained status message arrives the moment the worker subscribes,
        before any data message has registered the device. Updating a row
        that does not exist yet is silently lost, so the device stays at the
        schema's default until it happens to reconnect.

        Args:
            mac_address: The device MAC as it appears on the topic.

        Returns:
            The id of the `devices` row.
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
