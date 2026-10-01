"""Realtime publication membership (proxy for the dropped frontend delivery tests).

Realtime delivery itself is a Supabase platform concern, so this asserts the
precondition this repository owns: `measurements` and `devices` belong to the
`supabase_realtime` publication, without which no change event is streamed.
"""

from collections.abc import Callable

import pytest

pytestmark = pytest.mark.integration


def test_supabase_realtime_publishes_measurements_and_devices(
    query_scalar: Callable[[str], str],
) -> None:
    published = query_scalar(
        "SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY tablename) "
        "FROM pg_publication_tables WHERE pubname = 'supabase_realtime'"
    )

    assert "public.measurements" in published.split(",")
    assert "public.devices" in published.split(",")
