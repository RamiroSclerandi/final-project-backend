"""Worker configuration, validated from the environment at startup (see `.env.example`)."""

from typing import Literal
from uuid import uuid4

from pydantic import Field, PrivateAttr, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Startup configuration for the MQTT ingestion worker."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mqtt_host: str
    mqtt_user: str
    mqtt_password: SecretStr

    mqtt_port: int = Field(default=8883, gt=0, le=65535)
    mqtt_topic_data: str = "dl/v1/+/data"
    mqtt_topic_status: str = "dl/v1/+/status"
    mqtt_client_id_prefix: str = "ingest-worker"
    mqtt_ca_cert_path: str = ""

    supabase_url: str
    supabase_service_role_key: SecretStr

    ingest_queue_max: int = Field(default=1000, gt=0)
    mqtt_max_payload_bytes: int = Field(default=16384, gt=0)
    batch_max_size: int = Field(default=100, gt=0)
    batch_max_age_ms: int = Field(default=2000, gt=0)

    registry_cache_ttl_s: int = Field(default=900, gt=0)

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    _client_id_suffix: str = PrivateAttr(default_factory=lambda: uuid4().hex[:8])

    @field_validator("mqtt_password", "supabase_service_role_key")
    @classmethod
    def _reject_blank_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("secret must not be blank")
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _uppercase_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @property
    def mqtt_client_id(self) -> str:
        """Per-process id: overlapping deploys never share an id (MQTT-3.1.4-2)."""
        return f"{self.mqtt_client_id_prefix}-{self._client_id_suffix}"
