"""Resolve a `Reading` to its `sensor_id`, auto-registering unseen devices,
sensor types, and sensors, backed by a bounded-TTL cache.

See docs/SDD_Worker_Ingesta.md section 5.3 for the resolution algorithm and
sdd/worker-ingesta-mqtt/design's "Registry Caching" section for the caching
rationale. This is what lets a new node or a new channel register itself
without a code deploy — the spec requirement "Device and Sensor
Auto-Registration" (CA-3, CA-4).
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
    """One row of `sensor_types`, narrowed to what resolution needs."""

    id: str
    name: str
    unit: str


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

    Implements docs/SDD_Worker_Ingesta.md section 5.3: cache hit, else
    devices -> sensor_types -> sensors (SELECT, INSERT-ON-CONFLICT-DO-NOTHING,
    SELECT again), then cache and return.

    Threading contract: an instance is confined to the single writer thread
    that owns the pending batch and the Supabase client (see the
    architecture diagram in sdd/worker-ingesta-mqtt/design). It takes no
    lock and is not safe for concurrent use from more than one thread.

    Cache scope: only the resolved `sensor_id` is cached, keyed by
    `(mac, channel, unit, tag, source)` — never `devices.name`,
    `sensor_types.expected_min/max`, or any other mutable column, because
    the web platform's UI can edit those columns concurrently while ids are
    immutable by construction. A cached id is trusted for `ttl_seconds`
    (`REGISTRY_CACHE_TTL_S`); after that it expires and resolution runs
    again. This bounds the one real staleness risk of caching an id at all —
    that the row was deleted through the web platform in the meantime — to
    at most one TTL window. A rename or a threshold edit is never stale
    here, because this cache never reads those columns in the first place.
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
