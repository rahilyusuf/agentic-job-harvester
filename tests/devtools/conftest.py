"""Fail closed against network and secret resolution, including during lazy imports."""

import socket
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

from devtools.config import DATASET, LOCATION, PROJECT, SUBSCRIPTION, TABLE, TOPIC
from devtools.schemas import GCPStatus, PubSubStatus

TOKEN = "unit-test-secret-that-is-at-least-32-bytes"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")
    original = socket.socket.connect
    original_ex = socket.socket.connect_ex

    def guard(sock, address):
        if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1", "localhost"):
            pytest.fail("External network attempted in development unit test")
        return original(sock, address)

    def guard_ex(sock, address):
        if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1", "localhost"):
            pytest.fail("External network attempted in development unit test")
        return original_ex(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guard)
    monkeypatch.setattr(socket.socket, "connect_ex", guard_ex)
    import config.settings as settings_module

    settings = settings_module.Settings.model_construct(
        gcp_project_id=PROJECT,
        bq_dataset=DATASET,
        bq_location=LOCATION,
        litellm_proxy_base_url="http://localhost:4000",
        litellm_proxy_api_key="dummy",
        telegram_bot_token="dummy",
        e2b_api_key="dummy",
        apify_api_token="dummy",
    )
    monkeypatch.setattr(settings_module, "get_settings", lambda: settings)

    def forbidden(*args, **kwargs):
        pytest.fail("Secret Manager access attempted")

    monkeypatch.setattr(settings_module, "_fetch_secret", forbidden)


@pytest.fixture
def configured(monkeypatch):
    values = {
        "ENVIRONMENT": "development",
        "ENABLE_DEV_ENDPOINTS": "true",
        "DEV_AUTH_SECRET": TOKEN,
        "GCP_PROJECT_ID": PROJECT,
        "BQ_DATASET": DATASET,
        "BQ_LOCATION": LOCATION,
        "PUBSUB_EMBEDDING_TOPIC": TOPIC,
        "DEV_PUBSUB_SUBSCRIPTION": SUBSCRIPTION,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("K_SERVICE", raising=False)
    return {"X-Dev-Token": TOKEN}


@pytest.fixture
def fake_cloud(monkeypatch):
    from devtools import adapters

    pubsub = PubSubStatus(
        topic=f"projects/{PROJECT}/topics/{TOPIC}",
        subscription=f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}",
        active=True,
        pull_only=True,
        exclusive_subscription=True,
        ack_deadline_seconds=60,
        retention_seconds=86400,
    )
    cloud = MagicMock()
    cloud.gcp_status = AsyncMock(
        return_value=GCPStatus(
            project=PROJECT,
            dataset=DATASET,
            location=LOCATION,
            table=TABLE,
            verified=True,
            pubsub=pubsub,
        )
    )
    cloud.pubsub_status = AsyncMock(return_value=pubsub)
    cloud.insert_once = AsyncMock(side_effect=lambda jobs: [job.job_id for job in jobs])
    cloud.publish = AsyncMock(side_effect=lambda ids: (len(ids), 0))
    constructor = MagicMock(return_value=cloud)
    monkeypatch.setattr(adapters, "GoogleCloudOperations", constructor)
    cloud.constructor = constructor
    return cloud


@pytest.fixture
def app(configured):
    from devtools.app import create_app

    base = FastAPI()

    @base.get("/health")
    async def production_health():
        return {"status": "ok"}

    return create_app(base)
