"""Tests for startup configuration loading and validation.

Every value here is a dummy test value, never a real credential.
"""

import pytest
from pydantic import ValidationError

from ingest.config import Settings

REQUIRED_ENV = {
    "MQTT_HOST": "test.hivemq.cloud",
    "MQTT_USER": "worker",
    "MQTT_PASSWORD": "dummy-password",
    "SUPABASE_URL": "https://test.supabase.co",
    "SUPABASE_SERVICE_ROLE_KEY": "dummy-service-role-key",
}


@pytest.fixture
def valid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)


def test_loads_settings_when_every_required_variable_is_set(valid_env: None) -> None:
    settings = Settings()

    assert settings.mqtt_host == "test.hivemq.cloud"
    assert settings.mqtt_user == "worker"
    assert settings.supabase_url == "https://test.supabase.co"


def test_missing_service_role_key_fails_loudly_and_names_it(
    monkeypatch: pytest.MonkeyPatch, valid_env: None
) -> None:
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    with pytest.raises(ValidationError, match="supabase_service_role_key"):
        Settings()


def test_missing_mqtt_password_fails_loudly_and_names_it(
    monkeypatch: pytest.MonkeyPatch, valid_env: None
) -> None:
    monkeypatch.delenv("MQTT_PASSWORD", raising=False)

    with pytest.raises(ValidationError, match="mqtt_password"):
        Settings()


def test_missing_mqtt_host_fails_loudly_and_names_it(
    monkeypatch: pytest.MonkeyPatch, valid_env: None
) -> None:
    monkeypatch.delenv("MQTT_HOST", raising=False)

    with pytest.raises(ValidationError, match="mqtt_host"):
        Settings()


def test_blank_secret_is_rejected(monkeypatch: pytest.MonkeyPatch, valid_env: None) -> None:
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "")

    with pytest.raises(ValidationError, match="supabase_service_role_key"):
        Settings()


def test_negative_queue_max_is_rejected(monkeypatch: pytest.MonkeyPatch, valid_env: None) -> None:
    monkeypatch.setenv("INGEST_QUEUE_MAX", "-1")

    with pytest.raises(ValidationError, match="ingest_queue_max"):
        Settings()


def test_zero_batch_max_age_ms_is_rejected(
    monkeypatch: pytest.MonkeyPatch, valid_env: None
) -> None:
    monkeypatch.setenv("BATCH_MAX_AGE_MS", "0")

    with pytest.raises(ValidationError, match="batch_max_age_ms"):
        Settings()


def test_mqtt_port_out_of_range_is_rejected(
    monkeypatch: pytest.MonkeyPatch, valid_env: None
) -> None:
    monkeypatch.setenv("MQTT_PORT", "70000")

    with pytest.raises(ValidationError, match="mqtt_port"):
        Settings()


def test_default_numeric_bounds_match_env_example_when_unset(valid_env: None) -> None:
    settings = Settings()

    assert settings.mqtt_port == 8883
    assert settings.ingest_queue_max == 1000
    assert settings.mqtt_max_payload_bytes == 16384
    assert settings.batch_max_size == 100
    assert settings.batch_max_age_ms == 2000
    assert settings.registry_cache_ttl_s == 900


def test_mqtt_ca_cert_path_defaults_to_empty_string(valid_env: None) -> None:
    settings = Settings()

    assert settings.mqtt_ca_cert_path == ""


def test_secret_never_appears_in_repr(valid_env: None) -> None:
    settings = Settings()

    rendered = repr(settings)

    assert "dummy-password" not in rendered
    assert "dummy-service-role-key" not in rendered


def test_secret_never_appears_in_str(valid_env: None) -> None:
    settings = Settings()

    rendered = str(settings)

    assert "dummy-password" not in rendered
    assert "dummy-service-role-key" not in rendered


def test_client_id_prefix_default_matches_env_example(valid_env: None) -> None:
    settings = Settings()

    assert settings.mqtt_client_id_prefix == "ingest-worker"


def test_effective_client_id_combines_prefix_and_uuid_suffix(valid_env: None) -> None:
    settings = Settings()

    assert settings.mqtt_client_id.startswith("ingest-worker-")
    assert len(settings.mqtt_client_id) > len("ingest-worker-")


def test_effective_client_id_differs_across_instances(valid_env: None) -> None:
    first = Settings()
    second = Settings()

    assert first.mqtt_client_id != second.mqtt_client_id
    assert first.mqtt_client_id.startswith("ingest-worker-")
    assert second.mqtt_client_id.startswith("ingest-worker-")
