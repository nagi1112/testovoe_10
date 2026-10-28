from functools import lru_cache

from pydantic import Field, SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: SecretStr = SecretStr("local-development-key")
    outbox_poll_interval_seconds: float = Field(default=1, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    payment_min_delay_seconds: float = Field(default=2, ge=0)
    payment_max_delay_seconds: float = Field(default=5, gt=0)
    payment_failure_rate: float = Field(default=0.10, ge=0, le=1)
    webhook_timeout_seconds: float = Field(default=5, gt=0)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    log_level: str = "INFO"

    @field_validator("payment_max_delay_seconds")
    @classmethod
    def maximum_delay_must_exceed_minimum(cls, value: float, info: ValidationInfo) -> float:
        minimum = info.data.get("payment_min_delay_seconds", 0)
        if value < minimum:
            raise ValueError("payment_max_delay_seconds must be >= payment_min_delay_seconds")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()

