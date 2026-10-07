"""Convert a validated `datalogger.v1` payload into canonical readings."""

from dataclasses import dataclass
from datetime import UTC, datetime

from ingest.domain.payload import DataloggerV1


@dataclass(frozen=True)
class Reading:
    """One `measurements` row; a failed channel produces none, so `value` is never null."""

    device_mac: str
    channel: str
    unit: str
    tag: str
    source: str
    value: float
    value_min: float | None
    value_max: float | None
    sample_count: int | None
    recorded_at: datetime
    ts_source: str
    rssi: int
    seq: int
    boot: int
    lost: int | None = None
    store_drop: int | None = None


def normalize(payload: DataloggerV1, received_at: datetime) -> list[Reading]:
    """Convert one envelope into readings, skipping channels with `ok: false`."""
    if payload.ts == 0:
        recorded_at = received_at
        ts_source = "server"
    else:
        recorded_at = datetime.fromtimestamp(payload.ts, tz=UTC)
        # The time came from the device clock whatever meta.ts_src claims.
        ts_source = "device"

    # Meta.lost defaults to 0 for older firmware; only a reported value is stored.
    lost = payload.meta.lost if "lost" in payload.meta.model_fields_set else None
    store_drop = payload.meta.store.drop if payload.meta.store is not None else None

    readings: list[Reading] = []
    for channel in payload.ch:
        if not channel.ok:
            continue
        if channel.val is None:
            raise ValueError(
                f"Channel {channel.c!r} reports ok=true without a val field; "
                "the payload model should have rejected it before normalization"
            )
        readings.append(
            Reading(
                device_mac=payload.dev,
                channel=channel.c,
                unit=channel.u,
                tag=channel.t,
                source=channel.src,
                value=channel.val,
                value_min=channel.min,
                value_max=channel.max,
                sample_count=channel.n,
                recorded_at=recorded_at,
                ts_source=ts_source,
                rssi=payload.meta.rssi,
                seq=payload.seq,
                boot=payload.meta.boot,
                lost=lost,
                store_drop=store_drop,
            )
        )
    return readings
