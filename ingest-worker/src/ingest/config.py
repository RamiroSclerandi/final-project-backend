"""Worker configuration: loaded and validated from the environment at startup.

See `.env.example` for the full variable contract.
Every required variable that is missing, blank, or out of bounds raises a
`pydantic.ValidationError` naming the exact field before any MQTT or
Supabase connection is attempted. Secrets never have a default value, and
`SecretStr` keeps them out of `repr()`/`str()`.
"""

from uuid import uuid4

from pydantic import Field, PrivateAttr, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Startup configuration for the MQTT ingestion worker."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # MQTT broker — deployment-specific, no safe default.
    mqtt_host: str
    mqtt_user: str
    mqtt_password: SecretStr

    # MQTT broker — protocol-fixed or documented defaults (see .env.example).
    mqtt_port: int = Field(default=8883, gt=0, le=65535)
    mqtt_topic_data: str = "dl/v1/+/data"
    mqtt_topic_status: str = "dl/v1/+/status"
    mqtt_client_id_prefix: str = "ingest-worker"
    mqtt_ca_cert_path: str = ""

    # Supabase — deployment-specific, no safe default.
    supabase_url: str
    supabase_service_role_key: SecretStr

    # Ingestion queue and batching.
    ingest_queue_max: int = Field(default=1000, gt=0)
    mqtt_max_payload_bytes: int = Field(default=16384, gt=0)
    batch_max_size: int = Field(default=100, gt=0)
    batch_max_age_ms: int = Field(default=2000, gt=0)

    # Registry cache.
    registry_cache_ttl_s: int = Field(default=900, gt=0)

    log_level: str = "INFO"

    _client_id_suffix: str = PrivateAttr(default_factory=lambda: uuid4().hex[:8])

    @field_validator("mqtt_password", "supabase_service_role_key")
    @classmethod
    def _reject_blank_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("secret must not be blank")
        return value

    @property
    def mqtt_client_id(self) -> str:
        """Effective MQTT client id: prefix plus a per-process uuid4 suffix.

        Stable for the lifetime of this `Settings` instance. A unique suffix
        per process means an overlapping deploy can never share a client id,
        which avoids the MQTT-3.1.4-2 mutual disconnect loop.
        """
        return f"{self.mqtt_client_id_prefix}-{self._client_id_suffix}"
