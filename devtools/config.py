"""Explicit process configuration; never loads .env or production Settings."""

import os
from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, SecretStr

PROJECT = "agentic-job-harvester-v2"
DATASET = "job_intelligence"
LOCATION = "US"
TABLE = "raw_job_postings"
TOPIC = "job-embedding-queue"
SUBSCRIPTION = "job-embedding-test-sub"
CONFIRMATION = "WRITE_THREE_OR_FEWER_TEST_JOBS"


def endpoints_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return (
        env.get("ENVIRONMENT") == "development"
        and env.get("ENABLE_DEV_ENDPOINTS") == "true"
        and not env.get("K_SERVICE")
    )


class DevConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    token: SecretStr

    @classmethod
    def from_environment(cls) -> "DevConfig":
        token = os.environ.get("DEV_AUTH_SECRET", "")
        if not endpoints_enabled() or len(token.encode("utf-8")) < 32:
            raise ValueError("Explicit local development configuration and secret required")
        return cls(token=SecretStr(token))


def resources_configured() -> bool:
    """Compare deployment inputs with the fixed allowlist, never with request data."""
    expected = {
        "GCP_PROJECT_ID": PROJECT,
        "BQ_DATASET": DATASET,
        "BQ_LOCATION": LOCATION,
        "PUBSUB_EMBEDDING_TOPIC": TOPIC,
        "DEV_PUBSUB_SUBSCRIPTION": SUBSCRIPTION,
    }
    return all(os.environ.get(key) == value for key, value in expected.items())
