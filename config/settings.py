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
    gcp_region: str = Field("us-central1", description="Default Cloud Run / Vertex region")

    # ── BigQuery ───────────────────────────────────────────────────────────────
    bq_dataset: str = Field("job_intelligence", description="BigQuery dataset ID")
    bq_location: str = Field("US", description="BigQuery dataset location")

    # ── LiteLLM Proxy ─────────────────────────────────────────────────────────
    litellm_proxy_base_url: str = Field(
        ..., description="Base URL of the LiteLLM proxy (e.g. http://localhost:4000)"
    )
    litellm_proxy_api_key_secret: str = Field(
        "litellm-proxy-api-key",
        description="Secret Manager secret name for the LiteLLM proxy API key",
    )
    # Resolved at runtime via _resolve_litellm_api_key
    litellm_proxy_api_key: str = Field("", exclude=True)

    # ── LangFuse ──────────────────────────────────────────────────────────────
    langfuse_public_key: str = Field("", description="LangFuse public key (from env or Secret Mgr)")
    langfuse_secret_key: str = Field("", description="LangFuse secret key")
    langfuse_host: str = Field("https://cloud.langfuse.com")

    # ── Telegram ──────────────────────────────────────────────────────────────
    telegram_bot_token_secret: str = Field(
        "telegram-bot-token", description="Secret Manager secret name for Telegram bot token"
    )
    telegram_bot_token: str = Field("", exclude=True)

    # ── Firestore ─────────────────────────────────────────────────────────────
    firestore_database_id: str = Field(
        "(default)", description="Firestore database ID"
    )

    # ── Pub/Sub ───────────────────────────────────────────────────────────────
    pubsub_embedding_topic: str = Field(
        "job-embedding-queue", description="Pub/Sub topic for job embedding queue"
    )

    # ── E2B Sandbox ───────────────────────────────────────────────────────────
    e2b_api_key_secret: str = Field(
        "e2b-api-key", description="Secret Manager secret name for E2B API key"
    )
    e2b_api_key: str = Field("", exclude=True)
    e2b_sandbox_timeout_seconds: int = Field(
        30, ge=5, le=120, description="Hard timeout for E2B sandbox execution"
    )

    # ── Apify ─────────────────────────────────────────────────────────────────
    apify_webhook_secret: str = Field("", description="HMAC secret for Apify webhook validation")

    # ── Scoring Thresholds ─────────────────────────────────────────────────────
    notification_score_threshold: int = Field(
        85, ge=0, le=100,
        description="Minimum agreed_score to trigger Telegram notification (default per user)"
    )
    debate_score_delta_threshold: int = Field(
        10, ge=1, le=50,
        description="If |scorer - skeptic| > this, trigger round 2 of debate"
    )

    # ── Deduplication ─────────────────────────────────────────────────────────
    dedup_window_days: int = Field(90, ge=1, le=365, description="BQ dedup window in days")

    # ── Retrieval ─────────────────────────────────────────────────────────────
    vector_search_top_k: int = Field(50, ge=1, le=200, description="Step A: BQ VECTOR_SEARCH top K")
    prefilter_max_results: int = Field(15, ge=1, le=50, description="Step B: regex gate max output")
    prefilter_max_yoe: int = Field(5, ge=0, le=30, description="Maximum YOE allowed through prefilter")

    @field_validator("bq_location")
    @classmethod
    def bq_location_upper(cls, v: str) -> str:
        return v.upper()

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
