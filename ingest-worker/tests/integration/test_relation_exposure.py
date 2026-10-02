"""Access control on `v_latest_readings`, `mv_measurements_hourly`, and
`mv_measurements_daily` against a real, ephemeral Postgres + PostgREST pair.

Regression guard for two defects: all three relations granted
`anon` SELECT by default -- materialized views cannot carry row level
security at all, so the GRANT/REVOKE pair is their entire access-control
surface -- and `v_latest_readings` additionally ran with its owner's
privileges instead of the caller's, bypassing the RLS of every table it
joins. Both are fixed by the relation-exposure migration; this suite proves
it against the real database, not just against the migration's SQL text.
"""

import re
from collections.abc import Callable

import pytest
from postgrest.exceptions import APIError
from supabase import Client

pytestmark = pytest.mark.integration

_PROTECTED_RELATIONS = [
    "mv_measurements_hourly",
    "mv_measurements_daily",
    "v_latest_readings",
]

_PERMISSION_DENIED = "42501"


@pytest.mark.parametrize("relation", _PROTECTED_RELATIONS)
def test_anon_select_is_rejected(anon_client: Client, relation: str) -> None:
    with pytest.raises(APIError) as exc_info:
        anon_client.table(relation).select("*").limit(1).execute()
    assert exc_info.value.code == _PERMISSION_DENIED


@pytest.mark.parametrize("relation", _PROTECTED_RELATIONS)
def test_authenticated_select_succeeds(authenticated_client: Client, relation: str) -> None:
    response = authenticated_client.table(relation).select("*").limit(1).execute()
    assert isinstance(response.data, list)


def test_v_latest_readings_reports_security_invoker_enabled(
    view_reloptions: Callable[[str], str],
) -> None:
    reloptions = view_reloptions("v_latest_readings")
    # PostgreSQL stores this reloption as either `security_invoker=on` or
    # `security_invoker=true` depending on version and how it was set;
    # matching only one spelling can report a correctly fixed view as unfixed.
    assert re.search(r"security_invoker=(on|true)", reloptions), (
        f"expected security_invoker=on|true in reloptions, got: {reloptions!r}"
    )


def test_view_reloptions_rejects_a_name_that_is_not_an_identifier(
    view_reloptions: Callable[[str], str],
) -> None:
    with pytest.raises(ValueError, match="identifier"):
        view_reloptions("v_latest_readings' OR '1'='1")
