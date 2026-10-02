"""RED/GREEN tests converting a validated payload into canonical readings.

See docs/SDD_Worker_Ingesta.md section 5.2-5.3 and the spec requirements
"Unsynchronized Clock Handling" / "Failed Channel Produces No Row"
(sdd/worker-ingesta-mqtt/spec) for the behavior these tests enforce.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ingest.domain.normalize import normalize
from ingest.domain.payload import DataloggerV1

FIXTURES = Path(__file__).parent.parent / "fixtures"
RECEIVED_AT = datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC)


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())  # type: ignore[no-any-return]


def _full_envelope(channels_fixture_name: str) -> dict[str, Any]:
    base = _load_fixture("no_aggregation.json")
    fragment = _load_fixture(channels_fixture_name)
    return {**base, "ch": fragment["ch"]}


def test_normalizes_two_channels_from_no_aggregation_message() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert len(readings) == 2
    assert readings[0].channel == "temperature"
    assert readings[0].value == 21.12
    assert readings[1].channel == "pressure"
    assert readings[1].value == 1011.119


def test_device_timestamped_reading_uses_device_clock_and_ts_source() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings[0].ts_source == "device"
    assert readings[0].recorded_at == datetime.fromtimestamp(1788804294, tz=UTC)


def test_a_non_zero_device_ts_is_tagged_device_even_when_meta_says_server() -> None:
    envelope = _load_fixture("no_aggregation.json")
    envelope["meta"]["ts_src"] = "server"
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert all(reading.ts_source == "device" for reading in readings)
    assert all(
        reading.recorded_at == datetime.fromtimestamp(1788804294, tz=UTC) for reading in readings
    )


def test_readings_carry_the_reported_lost_counter_and_no_store_drop_without_store() -> None:
    envelope = json.loads((FIXTURES / "datalogger_v1" / "meta_lost_nonzero.json").read_text())
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert [(reading.lost, reading.store_drop) for reading in readings] == [(3, None)]


def test_an_explicit_zero_lost_is_stored_as_zero() -> None:
    envelope = json.loads((FIXTURES / "datalogger_v1" / "meta_lost_zero.json").read_text())
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings != []
    assert all(reading.lost == 0 for reading in readings)


def test_firmware_without_lost_leaves_it_null_and_keeps_the_store_drop() -> None:
    envelope = _load_fixture("no_aggregation.json")
    envelope["meta"]["store"]["drop"] = 5
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert all(reading.lost is None for reading in readings)
    assert all(reading.store_drop == 5 for reading in readings)


def test_ts_zero_stamps_server_arrival_time_and_marks_ts_source_server() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("ts_zero.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings != []
    assert all(reading.ts_source == "server" for reading in readings)
    assert all(reading.recorded_at == RECEIVED_AT for reading in readings)


def test_absent_tag_normalizes_to_empty_string_on_reading() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert all(reading.tag == "" for reading in readings)


def test_failed_channel_produces_no_reading() -> None:
    envelope = _full_envelope("failed_channel.json")
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings == []


def test_aggregated_channel_preserves_statistics() -> None:
    envelope = _full_envelope("aggregated.json")
    payload = DataloggerV1.model_validate(envelope)

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert len(readings) == 1
    reading = readings[0]
    assert reading.value == pytest.approx(21.05333)
    assert reading.value_min == pytest.approx(21.03)
    assert reading.value_max == pytest.approx(21.1)
    assert reading.sample_count == 6


def test_non_aggregated_channel_has_no_min_max_sample_count() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings[0].value_min is None
    assert readings[0].value_max is None
    assert readings[0].sample_count is None


def test_live_capture_pair_has_consecutive_seq_and_boot_with_no_gap() -> None:
    """Real broker capture (observation 329): consecutive seq 7/8, 15s apart.

    This is the negative case for later (boot, seq) gap detection (Phase 9,
    out of scope here) — this pair must never be flagged as a discontinuity.
    """
    first = DataloggerV1.model_validate(_load_fixture("live_capture_seq7.json"))
    second = DataloggerV1.model_validate(_load_fixture("live_capture_seq8.json"))

    readings_first = normalize(first, received_at=RECEIVED_AT)
    readings_second = normalize(second, received_at=RECEIVED_AT)

    assert readings_second[0].seq - readings_first[0].seq == 1
    assert readings_second[0].boot == readings_first[0].boot == 15
    assert readings_second[0].recorded_at - readings_first[0].recorded_at == timedelta(seconds=15)


def test_reading_carries_rssi_seq_and_boot_from_envelope() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    readings = normalize(payload, received_at=RECEIVED_AT)

    assert readings[0].rssi == -69
    assert readings[0].seq == 3
    assert readings[0].boot == 17
