"""config/settings.py — Central Settings object.

In production (Cloud Run), secrets are pulled from Secret Manager.
In local dev, values are read from .env via python-dotenv.

Usage:
    from config.settings import get_settings
    settings = get_settings()
    bq_client = bigquery.Client(project=settings.gcp_project_id)
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from google.cloud import secretmanager
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application-wide configuration, resolved from environment variables or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── GCP Core ──────────────────────────────────────────────────────────────
    gcp_project_id: str = Field(..., description="GCP project ID")
    gcp_region: str = Field(default="us-central1", description="Default Cloud Run / Vertex region")

    # ── BigQuery ───────────────────────────────────────────────────────────────
    bq_dataset: str = Field(default="job_intelligence", description="BigQuery dataset ID")
    bq_location: str = Field(default="US", description="BigQuery dataset location")

    # ── LiteLLM Proxy ─────────────────────────────────────────────────────────
    litellm_proxy_base_url: str = Field(
        ..., description="Base URL of the LiteLLM proxy (e.g. http://localhost:4000)"
    )
    litellm_proxy_api_key_secret: str = Field(
        default="litellm-proxy-api-key",
        description="Secret Manager secret name for the LiteLLM proxy API key",
    )
    # Resolved at runtime via _resolve_litellm_api_key
    litellm_proxy_api_key: str = Field(default="", exclude=True)

    # ── LangFuse ──────────────────────────────────────────────────────────────
    langfuse_public_key: str = Field(
        default="", description="LangFuse public key (from env or Secret Mgr)"
    )
    langfuse_secret_key: str = Field(default="", description="LangFuse secret key")
    langfuse_host: str = Field(default="https://cloud.langfuse.com")

    # ── Telegram ──────────────────────────────────────────────────────────────
    telegram_bot_token_secret: str = Field(
        default="telegram-bot-token",
        description="Secret Manager secret name for Telegram bot token",
    )
    telegram_bot_token: str = Field(default="", exclude=True)

    # ── Firestore ─────────────────────────────────────────────────────────────
    firestore_database_id: str = Field(default="(default)", description="Firestore database ID")

    # ── Pub/Sub ───────────────────────────────────────────────────────────────
    pubsub_embedding_topic: str = Field(
        default="job-embedding-queue", description="Pub/Sub topic for job embedding queue"
    )

    # ── E2B Sandbox ───────────────────────────────────────────────────────────
    e2b_api_key_secret: str = Field(
        default="e2b-api-key", description="Secret Manager secret name for E2B API key"
    )
    e2b_api_key: str = Field(default="", exclude=True)
    e2b_sandbox_timeout_seconds: int = Field(
        default=30, ge=5, le=120, description="Hard timeout for E2B sandbox execution"
    )

    # ── Environment ───────────────────────────────────────────────────────────
    environment: str = Field(
        default="development",
        description="Runtime environment: development | staging | production",
    )

    # ── Apify ─────────────────────────────────────────────────────────────────
    apify_webhook_secret: str = Field(
        default="", description="HMAC secret for Apify webhook validation"
    )
    apify_api_token_secret: str = Field(
        default="apify-api-token",
        description="Secret Manager secret name for the Apify API token",
    )
    # Resolved at runtime via resolve_secrets — never log this value
    apify_api_token: str = Field(default="", exclude=True)
    apify_allowed_actor_ids: list[str] = Field(
        default_factory=list,
        description="Allowlist of authorized Apify Actor IDs (empty allows all in dev)",
    )

    # ── Scoring Thresholds ─────────────────────────────────────────────────────
    notification_score_threshold: int = Field(
        default=85,
        ge=0,
        le=100,
        description="Minimum agreed_score to trigger Telegram notification (default per user)",
    )
    debate_score_delta_threshold: int = Field(
        default=10,
        ge=1,
        le=50,
        description="If |scorer - skeptic| > this, trigger round 2 of debate",
    )

    # ── Deduplication ─────────────────────────────────────────────────────────
    dedup_window_days: int = Field(default=90, ge=1, le=365, description="BQ dedup window in days")

    # ── Retrieval ─────────────────────────────────────────────────────────────
    vector_search_top_k: int = Field(
        default=50, ge=1, le=200, description="Step A: BQ VECTOR_SEARCH top K"
    )
    prefilter_max_results: int = Field(
        default=15, ge=1, le=50, description="Step B: regex gate max output"
    )
    prefilter_max_yoe: int = Field(
        default=5, ge=0, le=30, description="Maximum YOE allowed through prefilter"
    )

    @field_validator("bq_location")
    @classmethod
    def bq_location_upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("apify_allowed_actor_ids", mode="before")
    @classmethod
    def parse_allowed_actor_ids(cls, v: object) -> list[str]:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("[") and v.endswith("]"):
                import json

                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            return [item.strip() for item in v.split(",") if item.strip()]
        if isinstance(v, (list, tuple, set)):
            return [str(item).strip() for item in v if str(item).strip()]
        return []

    @property
    def is_production(self) -> bool:
        """Return True if running in a production environment."""
        return self.environment.lower() in ("production", "prod")

    @model_validator(mode="after")
    def resolve_secrets(self) -> "Settings":
        """Resolve Secret Manager secrets at startup if not already provided via env."""
        if not self.litellm_proxy_api_key:
            self.litellm_proxy_api_key = _fetch_secret(
                self.gcp_project_id, self.litellm_proxy_api_key_secret
            )
        if not self.telegram_bot_token:
            self.telegram_bot_token = _fetch_secret(
                self.gcp_project_id, self.telegram_bot_token_secret
            )
        if not self.e2b_api_key:
            self.e2b_api_key = _fetch_secret(self.gcp_project_id, self.e2b_api_key_secret)
        if not self.apify_api_token:
            self.apify_api_token = _fetch_secret(self.gcp_project_id, self.apify_api_token_secret)
        return self


def _fetch_secret(project_id: str, secret_name: str) -> str:
    """Fetch the latest version of a Secret Manager secret.

    Falls back to an empty string on any error (e.g., local dev without GCP credentials).
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception:  # noqa: BLE001
        # In local dev, secrets may be provided via .env directly
        return ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance.

    Uses lru_cache so Secret Manager is only called once per process lifetime.
    Call `get_settings.cache_clear()` in tests to reset.
    """
    return Settings()
