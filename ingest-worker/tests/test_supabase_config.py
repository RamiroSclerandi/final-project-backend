"""Static checks on `supabase/config.toml`, the source of the local/CD auth settings."""

import tomllib
from pathlib import Path

_CONFIG_PATH = Path(__file__).resolve().parents[2] / "supabase" / "config.toml"


def test_public_signup_is_disabled() -> None:
    # Proxy for the dropped frontend signUp test: accounts are provisioned by an
    # operator through the admin API, so the public signup path must stay closed.
    config = tomllib.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert config["auth"]["enable_signup"] is False
