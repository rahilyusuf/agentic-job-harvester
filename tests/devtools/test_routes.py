import os
import subprocess
import sys

import httpx
import pytest
from fastapi import FastAPI

from devtools.config import CONFIRMATION


@pytest.mark.parametrize("environment", [None, "", "production", "prod", "staging", "unknown"])
def test_routes_absent_outside_explicit_development(configured, monkeypatch, environment):
    from devtools.app import create_app

    if environment is None:
        monkeypatch.delenv("ENVIRONMENT")
    else:
        monkeypatch.setenv("ENVIRONMENT", environment)
    app = create_app(FastAPI())
    assert not any(path.startswith("/dev") for path in app.openapi()["paths"])


@pytest.mark.parametrize("flag", [None, "", "false", "1", "TRUE"])
def test_routes_absent_unless_flag_is_true(configured, monkeypatch, flag):
    from devtools.app import create_app

    if flag is None:
        monkeypatch.delenv("ENABLE_DEV_ENDPOINTS")
    else:
        monkeypatch.setenv("ENABLE_DEV_ENDPOINTS", flag)
    assert not any(path.startswith("/dev") for path in create_app(FastAPI()).openapi()["paths"])


def test_cloud_run_cannot_enable_local_tools(configured, monkeypatch):
    from devtools.app import create_app

    monkeypatch.setenv("K_SERVICE", "production-receiver")
    assert "/dev/health" not in create_app(FastAPI()).openapi()["paths"]


def test_enabled_without_secret_fails_startup(configured, monkeypatch):
    from devtools.app import create_app

    monkeypatch.delenv("DEV_AUTH_SECRET")
    with pytest.raises(ValueError):
        create_app(FastAPI())


@pytest.mark.asyncio
async def test_openapi_and_preview_are_local_only(app, configured, fake_cloud):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        schema = (await client.get("/openapi.json")).json()
        assert {path for path in schema["paths"] if path.startswith("/dev")} == {
            "/dev/health",
            "/dev/gcp/status",
            "/dev/ingestion/preview",
            "/dev/ingestion/execute",
            "/dev/pubsub/status",
        }
        assert (await client.get("/health")).json() == {"status": "ok"}
        assert (await client.get("/dev/health", headers=configured)).status_code == 200
        result = await client.post("/dev/ingestion/preview", headers=configured, json={})
        second = await client.post("/dev/ingestion/preview", headers=configured, json={})
        assert result.status_code == 200
        body = result.json()
        assert body == second.json()
        assert (
            body["received"],
            body["normalized"],
            body["unique"],
            body["in_batch_duplicates"],
        ) == (215, 215, 113, 102)
        assert body["rejected"] == 0
        assert len(body["selected"]) == 3
        assert len({job["job_id"] for job in body["selected"]}) == 3
    fake_cloud.constructor.assert_not_called()
    fake_cloud.insert_once.assert_not_awaited()
    fake_cloud.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers,body",
    [
        ({}, {"confirmation": CONFIRMATION}),
        ({"X-Dev-Token": "wrong"}, {"confirmation": CONFIRMATION}),
        (None, {}),
        (None, {"confirmation": "no"}),
    ],
)
async def test_execute_requires_auth_and_confirmation(app, configured, fake_cloud, headers, body):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured if headers is None else headers, json=body
        )
    assert result.status_code in (401, 422)
    fake_cloud.gcp_status.assert_not_awaited()
    fake_cloud.insert_once.assert_not_awaited()
    fake_cloud.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 4, 100, -1, True, "3"])
async def test_limits_rejected_before_cloud_use(app, configured, fake_cloud, limit):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute",
            headers=configured,
            json={"limit": limit, "confirmation": CONFIRMATION},
        )
    assert result.status_code == 422
    fake_cloud.gcp_status.assert_not_awaited()
    fake_cloud.insert_once.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        "GCP_PROJECT_ID",
        "BQ_DATASET",
        "BQ_LOCATION",
        "PUBSUB_EMBEDDING_TOPIC",
        "DEV_PUBSUB_SUBSCRIPTION",
    ],
)
async def test_wrong_configuration_rejected(app, configured, fake_cloud, monkeypatch, key):
    monkeypatch.setenv(key, "not-allowed")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == 409
    fake_cloud.constructor.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError("ambiguous"), RuntimeError("partial insert")])
async def test_no_retry_or_publish_after_uncertain_write(app, configured, fake_cloud, failure):
    fake_cloud.insert_once.side_effect = failure
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == 502
    fake_cloud.insert_once.assert_awaited_once()
    fake_cloud.publish.assert_not_awaited()
    assert "ambiguous" not in result.text and "partial insert" not in result.text


@pytest.mark.asyncio
async def test_only_confirmed_inserted_ids_published(app, configured, fake_cloud):
    selected = []

    async def subset(jobs):
        selected.extend(job.job_id for job in jobs)
        return selected[1:2]

    fake_cloud.insert_once.side_effect = subset
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == 200
    assert len(selected) == 3
    assert result.json()["inserted_ids"] == selected[1:2]
    fake_cloud.publish.assert_awaited_once_with(selected[1:2])


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted", [[], ["outside-selection"], ["duplicate", "duplicate"], None])
async def test_empty_or_invalid_confirmations_never_publish(app, configured, fake_cloud, inserted):
    fake_cloud.insert_once.side_effect = None
    fake_cloud.insert_once.return_value = inserted
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == (200 if inserted == [] else 502)
    fake_cloud.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_failure_prevents_insertion(app, configured, fake_cloud):
    fake_cloud.gcp_status.side_effect = ValueError("wrong dataset location")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == 409
    fake_cloud.insert_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_disable_blocks_existing_registered_routes(app, configured, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/dev/health", headers=configured)).status_code == 404


@pytest.mark.asyncio
async def test_disabled_routes_are_not_accessible(configured, monkeypatch):
    from devtools.app import create_app

    monkeypatch.delenv("ENABLE_DEV_ENDPOINTS")
    app = create_app(FastAPI())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/dev/health", headers=configured)).status_code == 404
        assert (
            await client.post(
                "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
            )
        ).status_code == 404


@pytest.mark.asyncio
async def test_remote_client_is_denied(app, configured, fake_cloud):
    transport = httpx.ASGITransport(app=app, client=("203.0.113.10", 12345))
    async with httpx.AsyncClient(transport=transport, base_url="http://local") as client:
        assert (await client.get("/dev/gcp/status", headers=configured)).status_code == 403
    fake_cloud.constructor.assert_not_called()


@pytest.mark.asyncio
async def test_status_endpoints_are_metadata_only(app, configured, fake_cloud):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/dev/gcp/status", headers=configured)).status_code == 200
        assert (await client.get("/dev/pubsub/status", headers=configured)).status_code == 200
    fake_cloud.gcp_status.assert_awaited_once()
    fake_cloud.pubsub_status.assert_awaited_once()
    fake_cloud.insert_once.assert_not_awaited()
    fake_cloud.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_body_cannot_override_resources(app, configured, fake_cloud):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute",
            headers=configured,
            json={"confirmation": CONFIRMATION, "project": "arbitrary"},
        )
    assert result.status_code == 422
    fake_cloud.insert_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_publish_when_subscription_changes_after_insert(app, configured, fake_cloud):
    fake_cloud.pubsub_status.side_effect = ValueError("embedding worker attached")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        result = await client.post(
            "/dev/ingestion/execute", headers=configured, json={"confirmation": CONFIRMATION}
        )
    assert result.status_code == 502
    fake_cloud.insert_once.assert_awaited_once()
    fake_cloud.publish.assert_not_awaited()


def test_local_app_reuses_actual_production_handlers(configured):
    from devtools.app import create_app
    from services.apify_receiver import apify_webhook

    app = create_app()
    assert "/webhook/apify" in app.openapi()["paths"]
    assert any(getattr(route, "endpoint", None) is apify_webhook for route in app.routes)


def test_production_import_never_imports_devtools():
    script = """
import os, sys, importlib.abc, socket
os.environ['LITELLM_LOCAL_MODEL_COST_MAP'] = 'True'
class NoDevTools(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'devtools' or fullname.startswith('devtools.'):
            raise AssertionError('Production imported development tooling')
sys.meta_path.insert(0, NoDevTools())
def no_network(*args, **kwargs):
    raise AssertionError('Network forbidden during production import check')
socket.socket.connect = no_network
socket.socket.connect_ex = no_network
from services.apify_receiver import app
assert not any(p.startswith('/dev') for p in app.openapi()['paths'])
assert '/webhook/apify' in app.openapi()['paths']
"""
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP")
    }
    environment.update(ENVIRONMENT="production", ENABLE_DEV_ENDPOINTS="true")
    result = subprocess.run(
        [sys.executable, "-X", "utf8", "-B", "-c", script],
        env=environment,
        capture_output=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
