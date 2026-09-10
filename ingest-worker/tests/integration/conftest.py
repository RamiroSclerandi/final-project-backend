"""Integration-test harness: a real, ephemeral Postgres + PostgREST pair.

Applies the actual migration and seed (never a rewritten copy of the schema)
against a bare `postgres:16` container, then serves it through a real
PostgREST container so tests exercise `SupabaseStore`/`MeasurementSink`
against the same HTTP interface the worker uses in production. A
Postgres-only harness would never catch a defect that lives in how
PostgREST reshapes a row (the `sensors.label` column) or in the write
semantics `supabase-py` gets over HTTP (a `source` NOT NULL violation, a
zero-row UPDATE returning 200) -- see tests/integration/test_supabase_store.py
and tests/integration/test_measurement_sink.py for the three production
defects this suite guards against.

Auth choice: supabase-py always sends `apikey`/`Authorization: Bearer <jwt>`
headers, and the worker always authenticates as `service_role` in
production. Rather than granting the anonymous role write access it should
never have (the real migration's RLS policies grant `authenticated` read
only, and no role an INSERT/UPDATE policy at all), PostgREST is configured
with a JWT secret and every test mints its own `service_role`-claim token
signed with it -- the same mechanism a real Supabase service_role key uses,
just minted locally instead of issued by the platform.
"""

import base64
import hashlib
import hmac
import json
import secrets
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from supabase import Client, create_client
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

from ingest.sink.supabase_sink import SupabaseStore

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SQL_DIR = Path(__file__).resolve().parent / "sql"
_MIGRATION_PATH = _REPO_ROOT / "supabase" / "migrations" / "20260909000000_initial_schema.sql"
_SEED_PATH = _REPO_ROOT / "supabase" / "seed.sql"

_POSTGRES_ALIAS = "postgres"
_POSTGREST_IMAGE = "postgrest/postgrest:v12.2.8"
_POSTGREST_PORT = 3000


def _run_sql_file(postgres: PostgresContainer, content: bytes, container_path: str) -> None:
    """Copy one SQL file into the Postgres container and apply it with `psql`.

    Runs inside the container itself instead of adding a Python Postgres
    driver dependency: bootstrap/migration/seed/roles are one-shot DDL the
    test process never needs to query directly.
    """
    postgres.copy_into_container(content, container_path)
    escaped_password = postgres.password.replace("'", "'\"'\"'")
    result = postgres.exec(
        [
            "sh",
            "-c",
            f"PGPASSWORD='{escaped_password}' psql --username {postgres.username} "
            f"--dbname {postgres.dbname} --host 127.0.0.1 -v ON_ERROR_STOP=1 -f {container_path}",
        ]
    )
    if result.exit_code != 0:
        raise RuntimeError(f"applying {container_path} failed:\n{result.output.decode()}")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _mint_jwt(secret: str, role: str) -> str:
    """Mint a minimal HS256 JWT carrying a `role` claim, signed with `secret`.

    Hand-rolled instead of adding a JWT dependency: HS256 is a five-line
    HMAC, and this is the only place the suite needs one. This mirrors what
    a real Supabase `service_role` key is -- a JWT whose `role` claim
    PostgREST's configured JWT secret verifies and switches into.
    """
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    now = int(time.time())
    claims = {
        "role": role,
        "iss": "ingest-worker-integration-tests",
        "iat": now,
        "exp": now + 3600,
    }
    payload = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{payload}".encode()
    signature = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    return f"{header}.{payload}.{_b64url(signature)}"


@pytest.fixture(scope="session")
def _postgrest_endpoint() -> Iterator[tuple[str, str]]:
    """Start an ephemeral Postgres + PostgREST pair; yield `(base_url, jwt_secret)`.

    Session-scoped: container startup dominates this suite's runtime, and
    every test uses a fresh device MAC / sensor tag / timestamp instead of a
    shared reset, so sharing the pair across tests is safe (see the
    `unique_mac` fixture).
    """
    jwt_secret = secrets.token_urlsafe(32)
    authenticator_password = secrets.token_urlsafe(24)

    with Network() as network:
        postgres = PostgresContainer("postgres:16", driver=None)
        postgres.with_network(network)
        postgres.with_network_aliases(_POSTGRES_ALIAS)
        with postgres:
            _run_sql_file(
                postgres,
                (_SQL_DIR / "00_bootstrap_auth.sql").read_bytes(),
                "/tmp/00_bootstrap_auth.sql",
            )
            _run_sql_file(postgres, _MIGRATION_PATH.read_bytes(), "/tmp/10_migration.sql")
            _run_sql_file(postgres, _SEED_PATH.read_bytes(), "/tmp/20_seed.sql")
            roles_sql = (
                (_SQL_DIR / "01_postgrest_roles.sql")
                .read_text()
                .replace("__AUTHENTICATOR_PASSWORD__", authenticator_password)
            )
            _run_sql_file(postgres, roles_sql.encode(), "/tmp/30_postgrest_roles.sql")

            postgrest = (
                DockerContainer(_POSTGREST_IMAGE)
                .with_network(network)
                .with_env(
                    "PGRST_DB_URI",
                    f"postgres://authenticator:{authenticator_password}"
                    f"@{_POSTGRES_ALIAS}:5432/{postgres.dbname}",
                )
                .with_env("PGRST_DB_SCHEMAS", "public")
                .with_env("PGRST_DB_ANON_ROLE", "anon")
                .with_env("PGRST_JWT_SECRET", jwt_secret)
                .with_exposed_ports(_POSTGREST_PORT)
                .waiting_for(HttpWaitStrategy(_POSTGREST_PORT, "/").for_status_code(200))
            )
            with postgrest:
                host = postgrest.get_container_host_ip()
                port = postgrest.get_exposed_port(_POSTGREST_PORT)
                yield f"http://{host}:{port}", jwt_secret


@pytest.fixture
def service_role_client(_postgrest_endpoint: tuple[str, str]) -> Client:
    """A real `supabase.Client`, authenticated as `service_role`, against the live PostgREST.

    supabase-py always targets `<base_url>/rest/v1`, which only exists
    behind Supabase's Kong gateway. There is no gateway in this harness --
    PostgREST serves its API at the root -- so the lazily-built postgrest
    client is repointed at that root; `.table()`/`.postgrest` are otherwise
    untouched real supabase-py/postgrest-py code making real HTTP calls.
    """
    base_url, jwt_secret = _postgrest_endpoint
    token = _mint_jwt(jwt_secret, role="service_role")
    client = create_client(base_url, token)
    client.rest_url = base_url  # type: ignore[assignment]
    return client


@pytest.fixture
def store(service_role_client: Client) -> SupabaseStore:
    """The `SupabaseStore` under test, wrapping the real PostgREST-backed client."""
    return SupabaseStore(service_role_client, source="hivemq")


@pytest.fixture
def unique_mac() -> str:
    """A fresh 12-hex-char uppercase MAC, satisfying `devices_mac_format` and unique per test."""
    return secrets.token_hex(6).upper()
