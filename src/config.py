"""Typed runtime configuration.

Values come from environment variables (injected into the pod from AWS Secrets
Manager via External Secrets) or, in local development only, from a `.env` file.

Settings are loaded lazily through `get_settings()` so that importing a module
never crashes a test runner or a tool that only needs part of the codebase.
"""
from __future__ import annotations

from functools import lru_cache
from typing import List, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Runtime ---
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True

    # --- Kafka ---
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RECONCILIATION: str = "sap-nightly-reconciliation-topic"
    KAFKA_TOPIC_HUMAN_DECISIONS: str = "sap-reconciliation-human-decisions"
    KAFKA_TOPIC_REVIEW_REQUESTS: str = "sap-reconciliation-review-requests"
    KAFKA_TOPIC_DLQ: str = "sap-nightly-reconciliation-dlq"
    KAFKA_CONSUMER_GROUP: str = "langgraph-supply-chain-workers"
    KAFKA_SECURITY_PROTOCOL: Literal["PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"] = "PLAINTEXT"
    KAFKA_SASL_MECHANISM: str | None = None  # e.g. SCRAM-SHA-512
    KAFKA_SASL_USERNAME: str | None = None
    KAFKA_SASL_PASSWORD: SecretStr | None = None
    KAFKA_SSL_CAFILE: str | None = None
    # LLM + SAP round-trips can be slow; must exceed worst-case processing time of ONE message
    KAFKA_MAX_POLL_INTERVAL_MS: int = Field(default=900_000, ge=60_000)

    # --- SAP S/4HANA (OData v2, OAuth2 client-credentials) ---
    SAP_BASE_URL: str = Field(default="http://localhost:50000", description="e.g. https://my-s4.example.com")
    SAP_TOKEN_URL: str = Field(default="http://localhost:50000/oauth/token")
    SAP_CLIENT_ID: str = "local-dev-client"
    SAP_CLIENT_SECRET: SecretStr
    SAP_TIMEOUT_SECONDS: float = Field(default=30.0, gt=0)
    SAP_VERIFY_TLS: bool = True
    SAP_DEFAULT_PURCHASE_ORDER_TYPE: str = "NB"

    # --- LLM ---
    OPENAI_API_KEY: SecretStr
    OPENAI_MODEL: str = "gpt-4o"
    OPENAI_TIMEOUT_SECONDS: float = 60.0
    OPENAI_MAX_RETRIES: int = 2
    # Used for the cost counter / cost alerting. Keep in sync with your contract pricing.
    OPENAI_INPUT_USD_PER_MTOK: float = 2.50
    OPENAI_OUTPUT_USD_PER_MTOK: float = 10.00

    # --- PagerDuty ---
    PAGERDUTY_INTEGRATION_KEY: SecretStr
    PAGERDUTY_EVENTS_URL: str = "https://events.pagerduty.com/v2/enqueue"

    # --- Graph persistence (required outside development) ---
    CHECKPOINT_DATABASE_URL: SecretStr | None = None

    # --- Business rules ---
    MAX_REVIEW_ITERATIONS: int = Field(default=5, ge=1, le=50)
    PRICE_SPIKE_THRESHOLD: float = Field(default=0.15, gt=0, lt=5)
    MAX_QUANTITY_PER_LINE: int = Field(default=1_000_000, gt=0)

    # --- Ports ---
    METRICS_PORT: int = 8000
    HEALTH_PORT: int = 8081
    LIVENESS_STALE_SECONDS: int = Field(
        default=1200,
        description="Liveness fails if the consume loop has not ticked for this long. "
        "Must be larger than worst-case single-message processing time.",
    )

    @model_validator(mode="after")
    def _enforce_production_invariants(self) -> AppSettings:
        if self.ENVIRONMENT != "development":
            if self.CHECKPOINT_DATABASE_URL is None:
                raise ValueError(
                    "CHECKPOINT_DATABASE_URL is required outside development: human-in-the-loop "
                    "state must survive pod restarts and KEDA scale-to-zero."
                )
            if not self.SAP_BASE_URL.startswith("https://") or not self.SAP_TOKEN_URL.startswith("https://"):
                raise ValueError("SAP_BASE_URL and SAP_TOKEN_URL must use https outside development.")
            if not self.SAP_VERIFY_TLS:
                raise ValueError("SAP_VERIFY_TLS cannot be disabled outside development.")
        return self

    @property
    def consumed_topics(self) -> List[str]:
        return [self.KAFKA_TOPIC_RECONCILIATION, self.KAFKA_TOPIC_HUMAN_DECISIONS]


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()  # type: ignore[call-arg]
