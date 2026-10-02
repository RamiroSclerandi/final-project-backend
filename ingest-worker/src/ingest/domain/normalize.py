"""Convert a validated `datalogger.v1` payload into canonical readings.

See docs/SDD_Worker_Ingesta.md section 5.2 for the frozen `Reading` shape and
section 5.3 for the resolution rules a `Reading` feeds into. A channel with
`ok: false` produces no `Reading` here — the sink layer (Phase 8, out of
scope for this module) archives the raw message independently of this
conversion, per the "Failed Channel Produces No Row" requirement.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from ingest.domain.payload import DataloggerV1


@dataclass(frozen=True)
class Reading:
    """One channel of one message: exactly one row of `measurements`.

    The shape deviates from SDD section 5.2, which declares `value: float | None`
    and a `valid: bool`. Both are vestiges of an earlier design in which a failed
    channel still produced a row. The schema rules that out twice over:
    `measurements.value` is NOT NULL, and there is no `valid` column at all.
    """

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


def normalize(payload: DataloggerV1, received_at: datetime) -> list[Reading]:
    """Convert one validated envelope into its canonical readings.

    Args:
        payload: A validated `datalogger.v1` envelope (guarantees `v == 1`).
        received_at: Server arrival time, used when the device clock is
            unsynchronized (`ts == 0`).

    Returns:
        One `Reading` per channel where `ok` is true, in payload order.
        Channels with `ok: false` are skipped entirely.
    """
    if payload.ts == 0:
        recorded_at = received_at
        ts_source = "server"
    else:
        recorded_at = datetime.fromtimestamp(payload.ts, tz=UTC)
        # The time came from the device clock whatever meta.ts_src claims (G-8, B-4).
        ts_source = "device"

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
            )
        )
    return readings
