"""RED/GREEN tests for the frozen `datalogger.v1` payload contract.

See docs/SDD_Worker_Ingesta.md section 2.4 for the field table these tests
enforce, and tests/fixtures/ for the real broker captures used as inputs.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ingest.domain.payload import DataloggerV1

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())  # type: ignore[no-any-return]


def _full_envelope(channels_fixture_name: str) -> dict[str, Any]:
    """Merge a channel-only fragment (section 2.2/2.3 style) onto a full envelope.

    `aggregated.json` and `failed_channel.json` are verbatim `{"ch": [...]}`
    fragments straight from the spec doc, not complete messages — the
    envelope fields (`v`, `dev`, `ts`, `seq`, `meta`) come from
    `no_aggregation.json`.
    """
    base = _load_fixture("no_aggregation.json")
    fragment = _load_fixture(channels_fixture_name)
    return {**base, "ch": fragment["ch"]}


def test_parses_no_aggregation_message() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    assert payload.v == 1
    assert payload.dev == "4022D83D6618"
    assert payload.seq == 3
    assert payload.meta.boot == 17
    assert len(payload.ch) == 2
    assert payload.ch[0].c == "temperature"
    assert payload.ch[0].val == 21.12
    assert payload.ch[1].c == "pressure"
    assert payload.ch[1].val == 1011.119


def test_parses_live_capture_pair() -> None:
    first = DataloggerV1.model_validate(_load_fixture("live_capture_seq7.json"))
    second = DataloggerV1.model_validate(_load_fixture("live_capture_seq8.json"))

    assert first.seq == 7
    assert second.seq == 8
    assert first.meta.boot == second.meta.boot == 15


def test_absent_tag_normalizes_to_empty_string() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    assert payload.ch[0].t == ""
    assert payload.ch[1].t == ""


def test_meta_rst_defaults_to_none_when_absent() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    assert payload.meta.rst is None


def test_ts_zero_message_keeps_restart_marker() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("ts_zero.json"))

    assert payload.ts == 0
    assert payload.meta.rst == "poweron"


def test_aggregated_channel_keeps_min_max_and_sample_count() -> None:
    envelope = _full_envelope("aggregated.json")

    payload = DataloggerV1.model_validate(envelope)

    channel = payload.ch[0]
    assert channel.val == pytest.approx(21.05333)
    assert channel.min == pytest.approx(21.03)
    assert channel.max == pytest.approx(21.1)
    assert channel.n == 6


def test_failed_channel_has_no_value() -> None:
    envelope = _full_envelope("failed_channel.json")

    payload = DataloggerV1.model_validate(envelope)

    channel = payload.ch[0]
    assert channel.ok is False
    assert channel.val is None


def test_rejects_version_other_than_one() -> None:
    envelope = _load_fixture("no_aggregation.json")
    envelope["v"] = 2

    with pytest.raises(ValidationError, match="unsupported datalogger contract version"):
        DataloggerV1.model_validate(envelope)


def test_rejects_lowercase_mac_address() -> None:
    envelope = _load_fixture("no_aggregation.json")
    envelope["dev"] = "4022d83d6618"

    with pytest.raises(ValidationError, match="12 uppercase hex"):
        DataloggerV1.model_validate(envelope)


def test_rejects_mac_address_with_wrong_length() -> None:
    envelope = _load_fixture("no_aggregation.json")
    envelope["dev"] = "4022D83D66"

    with pytest.raises(ValidationError, match="12 uppercase hex"):
        DataloggerV1.model_validate(envelope)


def test_rejects_val_present_when_ok_is_false() -> None:
    envelope = _full_envelope("failed_channel.json")
    envelope["ch"][0]["val"] = 21.0

    with pytest.raises(ValidationError, match="val must be absent"):
        DataloggerV1.model_validate(envelope)


def test_rejects_missing_val_when_ok_is_true() -> None:
    envelope = _load_fixture("no_aggregation.json")
    del envelope["ch"][0]["val"]

    with pytest.raises(ValidationError, match="val is required"):
        DataloggerV1.model_validate(envelope)


def test_rejects_n_equal_to_one() -> None:
    envelope = _full_envelope("aggregated.json")
    envelope["ch"][0]["n"] = 1

    with pytest.raises(ValidationError, match="greater than 1"):
        DataloggerV1.model_validate(envelope)


def test_rejects_min_max_without_n() -> None:
    envelope = _full_envelope("aggregated.json")
    del envelope["ch"][0]["n"]

    with pytest.raises(ValidationError, match=r"require ch\[\]\.n"):
        DataloggerV1.model_validate(envelope)
