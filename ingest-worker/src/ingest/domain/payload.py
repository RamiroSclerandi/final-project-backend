"""Pydantic models for the frozen `datalogger.v1` MQTT payload contract.

See docs/SDD_Worker_Ingesta.md section 2.4 for the frozen field table this
module enforces: reject any `v` other than 1, `dev` must be 12 uppercase hex
characters, `ch[].val` is required exactly when `ch[].ok` is true, and
`ch[].min`/`max`/`n` only appear together when `n > 1`.
"""

import re

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

_MAC_PATTERN = re.compile(r"^[0-9A-F]{12}$")


class Store(BaseModel):
    """On-device buffer state (`meta.store`)."""

    model_config = ConfigDict(extra="forbid")

    k: str
    pct: int
    pend: int
    drop: int


class Meta(BaseModel):
    """Message metadata (`meta`)."""

    model_config = ConfigDict(extra="forbid")

    rssi: int
    fw: str
    boot: int
    ts_src: str
    store: Store
    rst: str | None = None


class Channel(BaseModel):
    """One sensor channel (`ch[]`)."""

    model_config = ConfigDict(extra="forbid")

    c: str
    u: str
    t: str = ""
    src: str
    ok: bool
    val: float | None = None
    min: float | None = None
    max: float | None = None
    n: int | None = None

    @model_validator(mode="after")
    def _check_val_matches_ok(self) -> "Channel":
        if self.ok and self.val is None:
            raise ValueError("ch[].val is required when ok is true")
        if not self.ok and self.val is not None:
            raise ValueError("ch[].val must be absent when ok is false")
        return self

    @model_validator(mode="after")
    def _check_aggregation_fields(self) -> "Channel":
        if self.n is not None and self.n <= 1:
            raise ValueError("ch[].n must be greater than 1 when present")
        if (self.min is not None or self.max is not None) and self.n is None:
            raise ValueError("ch[].min/max require ch[].n to be present")
        return self


class DataloggerV1(BaseModel):
    """The frozen `datalogger.v1` envelope published on `dl/v1/{MAC}/data`."""

    model_config = ConfigDict(extra="forbid")

    v: int
    dev: str
    ts: int
    seq: int
    meta: Meta
    ch: list[Channel]

    @field_validator("v")
    @classmethod
    def _reject_unknown_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError(f"unsupported datalogger contract version: {value}")
        return value

    @field_validator("dev")
    @classmethod
    def _validate_mac(cls, value: str) -> str:
        if not _MAC_PATTERN.match(value):
            raise ValueError(f"dev must be 12 uppercase hex characters, got {value!r}")
        return value
