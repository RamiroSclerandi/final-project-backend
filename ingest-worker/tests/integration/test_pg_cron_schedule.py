"""pg_cron refresh schedule (port of the dropped frontend `aggregation-schedule` suite).

The shared harness runs a bare `postgres:16`, which has no pg_cron, so the
migration's guarded schedule block is skipped there. This module starts its
own container from the Supabase Postgres image -- the one the local CLI stack
uses, with pg_cron preloaded and the `anon`/`authenticated`/`service_role`
roles and `auth.users` already in place -- applies the real migrations to it,
and asserts the schedule that actually lands in `cron.job`.

The image major version tracks `[db] major_version` in supabase/config.toml.
"""

import secrets
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer

pytestmark = pytest.mark.integration

_MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "supabase" / "migrations"
_SUPABASE_POSTGRES_IMAGE = "public.ecr.aws/supabase/postgres:17.6.1.167"
_READY_TIMEOUT_SECONDS = 120
_DB_USER = "postgres"
_DB_NAME = "postgres"


def _psql_command(password: str, *args: str) -> list[str]:
    """Build an argv running `psql` over TCP inside the container, without a shell."""
    return [
        "env",
        f"PGPASSWORD={password}",
        "psql",
        "--username",
        _DB_USER,
        "--dbname",
        _DB_NAME,
        "--host",
        "127.0.0.1",
        *args,
    ]


def _wait_until_ready(container: DockerContainer, password: str) -> None:
    """Poll until the final server accepts password logins over TCP.

    The image's entrypoint first runs a temporary socket-only server for its
    init scripts, then restarts; a TCP login as the application role only
    succeeds after that restart, so it is a stricter signal than a log line.
    """
    deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        result = container.exec(_psql_command(password, "-tAc", "SELECT 1"))
        if result.exit_code == 0:
            return
        time.sleep(1)
    raise RuntimeError(
        f"{_SUPABASE_POSTGRES_IMAGE} did not accept TCP logins within {_READY_TIMEOUT_SECONDS}s"
    )


def _apply_migration(container: DockerContainer, password: str, path: Path) -> None:
    """Copy one migration into the container and apply it, stopping at the first error."""
    container_path = f"/tmp/{path.name}"
    container.copy_into_container(path.read_bytes(), container_path)
    result = container.exec(_psql_command(password, "-v", "ON_ERROR_STOP=1", "-f", container_path))
    if result.exit_code != 0:
        raise RuntimeError(f"applying {path.name} failed:\n{result.output.decode()}")


@pytest.fixture(scope="module")
def cron_query() -> Iterator[Callable[[str], str]]:
    """Yield a function running one read-only SQL query against a migrated Supabase Postgres.

    Module-scoped and independent of the session harness: only this module
    pays for the larger image, and every other test keeps using `postgres:16`.
    Pass constant queries only; the SQL is one argv element with no binding.
    """
    password = secrets.token_urlsafe(24)
    container = DockerContainer(_SUPABASE_POSTGRES_IMAGE).with_env("POSTGRES_PASSWORD", password)

    with container:
        _wait_until_ready(container, password)
        for migration_path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            _apply_migration(container, password, migration_path)

        def _query(sql: str) -> str:
            result = container.exec(_psql_command(password, "-tAc", sql))
            if result.exit_code != 0:
                raise RuntimeError(f"query failed: {sql}\n{result.output.decode()}")
            return result.output.decode().strip()

        yield _query


def test_pg_cron_extension_is_installed(cron_query: Callable[[str], str]) -> None:
    installed = cron_query("SELECT count(*) FROM pg_extension WHERE extname = 'pg_cron'")

    assert installed == "1"


def test_scheduled_jobs_are_registered_and_active(cron_query: Callable[[str], str]) -> None:
    jobs = cron_query(
        "SELECT jobname || '|' || schedule || '|' || active FROM cron.job ORDER BY jobname"
    )

    assert jobs.splitlines() == [
        "purge-raw-messages|30 3 * * *|true",
        "refresh-daily|10 0 * * *|true",
        "refresh-hourly|5 * * * *|true",
    ]


_RETENTION = "20261002120000_raw_messages_retention.sql"


def test_retention_rollback_unschedules_the_purge_and_reapplying_restores_it(
    cron_query: Callable[[str], str],
) -> None:
    purge_jobs = "SELECT count(*) FROM cron.job WHERE jobname = 'purge-raw-messages'"
    rollback_sql = (_MIGRATIONS_DIR.parent / "rollbacks" / _RETENTION).read_text()
    migration_sql = (_MIGRATIONS_DIR / _RETENTION).read_text()

    cron_query(rollback_sql)
    try:
        assert cron_query(purge_jobs) == "0"
    finally:
        cron_query(migration_sql)

    assert cron_query(purge_jobs) == "1"
