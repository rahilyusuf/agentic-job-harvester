"""Opt-in aborted-source ingestion; all transports and persistence are mocked."""

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from config.settings import Settings
from services import apify_receiver as receiver
from services.normalizers.batch import normalize_batch
from tests.services.test_apify_receiver import (
    _build_mock_request,
)
from tests.services.test_apify_receiver import (
    group1_dataset as group1_dataset,
)
from tests.services.test_apify_receiver import (
    group1_ingestion as group1_ingestion,
)


@pytest.fixture
def aborted(group1_ingestion):
    payload, fetch, repo, publish = group1_ingestion
    payload["eventType"] = "ACTOR.RUN.ABORTED"
    payload["resource"]["status"] = "ABORTED"
    receiver.get_settings().apify_ingest_aborted_runs = True
    return payload, fetch, repo, publish


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [None, False])
async def test_aborted_disabled_without_external_operations(aborted, enabled):
    payload, fetch, repo, publish = aborted
    settings = receiver.get_settings()
    if enabled is None:
        settings = Settings.model_construct()
        # Include only defaults and the values needed before eligibility checks.
        settings.environment = "development"
    else:
        settings.apify_ingest_aborted_runs = enabled
    assert settings.apify_ingest_aborted_runs is False
    with patch.object(receiver, "get_settings", return_value=settings):
        with patch.object(receiver, "_verify_actor_run") as verify:
            result = await receiver.apify_webhook(_build_mock_request(payload))
    assert result["received"] == "ACTOR.RUN.ABORTED"
    assert result["inserted"] == result["published"] == 0
    assert "disabled" in result["message"]
    verify.assert_not_called()
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("omit_status", [False, True])
async def test_aborted_full_fixture(aborted, group1_dataset, omit_status):
    payload, fetch, repo, publish = aborted
    verified = dict(payload["resource"])
    if omit_status:
        payload["resource"].pop("status")
    fetch.return_value = group1_dataset
    with patch.object(receiver, "_verify_actor_run", AsyncMock(return_value=verified)):
        result = await receiver.apify_webhook(_build_mock_request(payload))
    assert result == {
        "dataset_id": "dataset-group1",
        "actor_run_id": "run-group1",
        "received": 215,
        "normalized": 215,
        "rejected": 0,
        "unique": 113,
        "in_batch_duplicates": 102,
        "apply_url_fallback": 89,
        "inserted": 113,
        "published": 113,
        "publish_failures": 0,
        "recovered": 0,
    }
    jobs = repo.batch_insert_jobs.await_args.args[0]
    publish.assert_awaited_once_with(
        [job.job_id for job in jobs], project_id="test-project", topic="test-topic"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_status,verified_status",
    [
        (event, verified)
        for event in ("SUCCEEDED", "ABORTED")
        for verified in (
            "SUCCEEDED",
            "ABORTED",
            "RUNNING",
            "ABORTING",
            "FAILED",
            "TIMED-OUT",
            None,
            "UNKNOWN",
        )
        if event != verified
    ],
)
async def test_verified_status_must_match_event(aborted, event_status, verified_status):
    payload, fetch, repo, publish = aborted
    payload["eventType"] = f"ACTOR.RUN.{event_status}"
    payload["resource"]["status"] = event_status
    verified = {**payload["resource"], "status": verified_status}
    with patch.object(receiver, "_verify_actor_run", AsyncMock(return_value=verified)):
        with pytest.raises(receiver.HTTPException) as error:
            await receiver.apify_webhook(_build_mock_request(payload))
    assert error.value.status_code == 422
    assert f"expected '{event_status}'" in error.value.detail
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_id", [None, "", "   ", 42, [], {}, "wrong-dataset"])
async def test_aborted_requires_verified_dataset(aborted, dataset_id):
    payload, fetch, repo, publish = aborted
    verified = {**payload["resource"], "defaultDatasetId": dataset_id}
    with patch.object(receiver, "_verify_actor_run", AsyncMock(return_value=verified)):
        with pytest.raises(receiver.HTTPException) as error:
            await receiver.apify_webhook(_build_mock_request(payload))
    assert error.value.status_code == 422
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "RUNNING", None])
async def test_conflicting_payload_status_is_ignored(aborted, status):
    payload, fetch, repo, publish = aborted
    payload["resource"]["status"] = status
    with patch.object(receiver, "_verify_actor_run") as verify:
        result = await receiver.apify_webhook(_build_mock_request(payload))
    assert result["inserted"] == result["published"] == 0
    verify.assert_not_called()
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("items,rejected", [([], 0), ([None, {}, "invalid"], 3)])
async def test_empty_or_invalid_aborted_dataset_does_not_write(aborted, items, rejected):
    payload, fetch, repo, publish = aborted
    fetch.return_value = items
    result = await receiver.apify_webhook(_build_mock_request(payload))
    assert result["received"] == len(items) and result["rejected"] == rejected
    assert result["inserted"] == result["published"] == 0
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "authentication", "payload_actor", "verified_actor"])
async def test_aborted_preserves_production_security(aborted, failure):
    payload, fetch, repo, publish = aborted
    settings = receiver.get_settings()
    settings.environment = "production"
    settings.apify_webhook_secret = "dummy-webhook-secret"
    settings.apify_allowed_actor_ids = ["actor-group1"]
    verified = dict(payload["resource"])
    if failure == "payload_actor":
        payload["resource"]["actId"] = "unauthorized"
    if failure == "verified_actor":
        verified["actId"] = "unauthorized"
    body = json.dumps(payload).encode()
    signature = hmac.new(settings.apify_webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    headers = [
        (b"x-apify-signature", ("invalid" if failure == "authentication" else signature).encode())
    ]
    fetch.return_value = []
    with patch.object(receiver, "_verify_actor_run", AsyncMock(return_value=verified)) as verify:
        if failure:
            with pytest.raises(receiver.HTTPException) as error:
                await receiver.apify_webhook(_build_mock_request(body, headers=headers))
            assert error.value.status_code == (401 if failure == "authentication" else 403)
            fetch.assert_not_awaited()
        else:
            result = await receiver.apify_webhook(_build_mock_request(body, headers=headers))
            assert result["received"] == 0
        if failure in ("authentication", "payload_actor"):
            verify.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.parametrize(
    "value,expected", [(None, False), ("false", False), ("true", True), ("invalid", None)]
)
def test_aborted_setting_environment_parsing(monkeypatch, value, expected):
    monkeypatch.delenv("APIFY_INGEST_ABORTED_RUNS", raising=False)
    if value is not None:
        monkeypatch.setenv("APIFY_INGEST_ABORTED_RUNS", value)
    def make_settings() -> Settings:
        return Settings(
            _env_file=None,
            gcp_project_id="test-project",
            litellm_proxy_base_url="http://localhost:4000",
            litellm_proxy_api_key="dummy",
            telegram_bot_token="dummy",
            e2b_api_key="dummy",
            apify_api_token="dummy",
        )
    with patch("config.settings._fetch_secret", side_effect=AssertionError("No secret calls")):
        if expected is None:
            with pytest.raises(ValidationError):
                make_settings()
        else:
            assert make_settings().apify_ingest_aborted_runs is expected


@pytest.mark.asyncio
@pytest.mark.parametrize("secret", ["dummy-secret", ""])
async def test_authentication_precedes_disabled_aborted_gate(aborted, secret):
    payload, fetch, repo, publish = aborted
    settings = receiver.get_settings()
    settings.environment = "production"
    settings.apify_webhook_secret = secret
    settings.apify_ingest_aborted_runs = False
    with patch.object(receiver, "_verify_actor_run") as verify:
        with pytest.raises(receiver.HTTPException) as error:
            await receiver.apify_webhook(_build_mock_request(payload))
    assert error.value.status_code == (401 if secret else 500)
    verify.assert_not_called()
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_aborted_full_fixture_isolates_invalid_first_occurrence(aborted, group1_dataset):
    payload, fetch, repo, publish = aborted
    fetch.return_value = [
        None,
        {**group1_dataset[0], "jobId": []},
        {**group1_dataset[0], "companyName": []},
        *group1_dataset,
        {**group1_dataset[0], "title": "Later duplicate"},
    ]
    result = await receiver.apify_webhook(_build_mock_request(payload))
    assert result["received"] == 219 and result["normalized"] == 216
    assert result["rejected"] == 3 and result["unique"] == 113
    assert result["in_batch_duplicates"] == 103
    expected = normalize_batch(group1_dataset).jobs
    actual = repo.batch_insert_jobs.await_args.args[0]
    assert [job.model_dump(exclude={"scraped_at"}) for job in actual] == [
        job.model_dump(exclude={"scraped_at"}) for job in expected
    ]
    assert publish.await_count == 1


@pytest.mark.asyncio
async def test_aborted_repeated_delivery_preserves_recovery(aborted, group1_dataset):
    payload, fetch, repo, publish = aborted
    fetch.return_value = group1_dataset
    ids = [job.job_id for job in normalize_batch(group1_dataset).jobs]
    repo.batch_insert_jobs.side_effect = [ids, []]
    repo.get_unembedded_job_ids.return_value = ids[:1]
    first = await receiver.apify_webhook(_build_mock_request(payload))
    second = await receiver.apify_webhook(_build_mock_request(payload))
    assert first["inserted"] == 113 and first["published"] == 113
    assert second["inserted"] == 0 and second["recovered"] == second["published"] == 1
    assert publish.await_args.args[0] == ids[:1]
    repo.get_unembedded_job_ids.assert_awaited_once_with(job_ids=ids)
