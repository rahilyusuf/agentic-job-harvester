"""tests/services/test_apify_receiver.py — Integration and stage flow tests for apify_receiver."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from config.settings import Settings
from services.apify_receiver import (
    _fetch_dataset_items,
    _verify_actor_run,
    apify_webhook,
    reconcile_unpublished_jobs,
    trigger_reconciliation,
)


def _make_test_settings(**overrides: Any) -> Settings:
    """Construct a valid Settings instance for tests without hitting Secret Manager."""
    data: dict[str, Any] = {
        "gcp_project_id": "test-project",
        "litellm_proxy_base_url": "http://localhost:4000",
        "apify_webhook_secret": "",
        "apify_api_token": "dummy-token",
        "pubsub_embedding_topic": "test-topic",
        "environment": "development",
    }
    data.update(overrides)
    return Settings.model_construct(**data)


def _build_mock_request(
    payload: dict[str, Any] | bytes | str,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    if isinstance(payload, bytes):
        body = payload
    elif isinstance(payload, str):
        body = payload.encode("utf-8")
    else:
        body = json.dumps(payload).encode("utf-8")

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    req_headers = [(b"content-type", b"application/json")]
    if headers:
        req_headers.extend(headers)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/webhook/apify",
        "headers": req_headers,
    }
    return Request(scope, receive)


@pytest.fixture
def mock_dataset_items() -> list[dict[str, Any]]:
    return [
        # Item 1: Direct jobUrl
        {
            "title": "Data Engineer 1",
            "companyName": "Company A",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": "https://example.com/job1",
            "applyUrl": "https://example.com/apply1",
        },
        # Item 2: Direct jobUrl (no applyUrl)
        {
            "title": "Data Engineer 2",
            "companyName": "Company B",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": "https://example.com/job2",
            "applyUrl": "",
        },
        # Item 3: Empty jobUrl, valid applyUrl -> fallback 1
        {
            "title": "Workday Engineer 1",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": "",
            "applyUrl": "https://workday.example.com/job3",
        },
        # Item 4: Whitespace jobUrl, valid applyUrl -> fallback 2
        {
            "title": "Workday Engineer 2",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": "   ",
            "applyUrl": "https://workday.example.com/job4",
        },
        # Item 5: None jobUrl, valid applyUrl -> fallback 3
        {
            "title": "Workday Engineer 3",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": None,
            "applyUrl": "https://workday.example.com/job5",
        },
        # Item 6: Both missing -> skipped
        {
            "title": "Invalid Job",
            "companyName": "Nowhere",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily."
            * 2,
            "jobUrl": "",
            "applyUrl": "",
        },
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_receiver_tracks_apply_url_fallback_and_logs(
    mock_dataset_items: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
):
    """Verify that apify_webhook accurately counts applyUrl fallbacks and logs stage events."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-test-123",
            "actId": "scraper-actor",
            "defaultDatasetId": "dataset-xyz",
        },
    }
    request = _build_mock_request(payload)

    mock_settings = _make_test_settings()

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(
                return_value={
                    "id": "run-test-123",
                    "actId": "scraper-actor",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "dataset-xyz",
                }
            ),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=mock_dataset_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(5, 0))),
        caplog.at_level(logging.INFO, logger="ingestion.events"),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["id1", "id2", "id3", "id4", "id5"])

        response = await apify_webhook(request)

    # 1. Assert response structure & counts
    assert response["dataset_id"] == "dataset-xyz"
    assert response["received"] == 6
    assert response["normalized"] == 5
    assert response["rejected"] == 1
    assert response["unique"] == 5
    assert response["in_batch_duplicates"] == 0
    assert response["apply_url_fallback"] == 3
    assert response["inserted"] == 5
    assert response["published"] == 5

    # 2. Assert semantic stage flow and counters, independent of console formatting.
    events = [record.event_fields for record in caplog.records if hasattr(record, "event_fields")]
    names = [event["event"] for event in events]
    milestones = [
        "ingestion.webhook.received",
        "ingestion.actor.verified",
        "ingestion.dataset.fetched",
        "ingestion.normalization.completed",
        "ingestion.bigquery.completed",
        "ingestion.recovery.completed",
        "ingestion.pubsub.completed",
        "ingestion.completed",
    ]
    assert [names.index(name) for name in milestones] == sorted(
        names.index(name) for name in milestones
    )
    final = events[-1]
    assert final["received"] == 6 and final["normalized"] == 5
    assert final["rejected"] == 1 and final["apply_url_fallback"] == 3
    assert final["unique"] == 5 and final["in_batch_duplicates"] == 0
    assert len({event["request_id"] for event in events}) == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_successful_insertion_and_successful_publication():
    """BigQuery insertion succeeds and all Pub/Sub messages are published."""
    single_item = [
        {
            "title": "Machine Learning Engineer",
            "companyName": "AI Corp",
            "descriptionText": "Develop state of the art deep learning and generative models for production."
            * 2,
            "jobUrl": "https://example.com/job/ml-1",
        }
    ]
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-1",
            "actId": "act-1",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-1",
        },
    }
    request = _build_mock_request(payload)
    mock_settings = _make_test_settings(
        apify_api_token="dummy",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(
                return_value={
                    "id": "run-1",
                    "actId": "act-1",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "ds-1",
                }
            ),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items", new=AsyncMock(return_value=single_item)
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(1, 0))
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["job-success-1"])

        response = await apify_webhook(request)

    assert response["inserted"] == 1
    assert response["published"] == 1
    assert response["publish_failures"] == 0
    assert response["recovered"] == 0
    mock_pub.assert_awaited_once_with(["job-success-1"], project_id="test-p", topic="test-t")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bigquery_insertion_succeeds_but_pubsub_publication_fails():
    """BigQuery insertion succeeds, but Pub/Sub publication fails for all jobs."""
    single_item = [
        {
            "title": "MLOps Engineer",
            "companyName": "Cloud Inc",
            "descriptionText": "Deploy ML pipelines and infrastructure on modern cloud architectures."
            * 2,
            "jobUrl": "https://example.com/job/mlops-1",
        }
    ]
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-2",
            "actId": "act-2",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-2",
        },
    }
    request = _build_mock_request(payload)
    mock_settings = _make_test_settings(
        apify_api_token="dummy",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(
                return_value={
                    "id": "run-2",
                    "actId": "act-2",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "ds-2",
                }
            ),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items", new=AsyncMock(return_value=single_item)
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(0, 1))),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["job-fail-1"])

        response = await apify_webhook(request)

    assert response["inserted"] == 1
    assert response["published"] == 0
    assert response["publish_failures"] == 1
    assert response["recovered"] == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_partial_publication_failure():
    """BigQuery inserts 2 jobs; Pub/Sub publishes 1 and fails 1."""
    items = [
        {
            "title": "Backend Engineer",
            "companyName": "Tech A",
            "descriptionText": "Write performant asynchronous backend services using python and fastapi."
            * 2,
            "jobUrl": "https://example.com/job/be-1",
        },
        {
            "title": "Data Scientist",
            "companyName": "Tech B",
            "descriptionText": "Perform exploratory data analysis and build statistical machine learning models."
            * 2,
            "jobUrl": "https://example.com/job/ds-1",
        },
    ]
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-3",
            "actId": "act-3",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-3",
        },
    }
    request = _build_mock_request(payload)
    mock_settings = _make_test_settings(
        apify_api_token="dummy",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(
                return_value={
                    "id": "run-3",
                    "actId": "act-3",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "ds-3",
                }
            ),
        ),
        patch("services.apify_receiver._fetch_dataset_items", new=AsyncMock(return_value=items)),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(1, 1))),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["job-part-1", "job-part-2"])

        response = await apify_webhook(request)

    assert response["inserted"] == 2
    assert response["published"] == 1
    assert response["publish_failures"] == 1
    assert response["recovered"] == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_retry_after_jobs_are_already_stored():
    """Webhook retries for a batch already stored in BQ; discovers unembedded NEW_RAW jobs and publishes them."""
    items = [
        {
            "title": "Computer Vision Engineer",
            "companyName": "Vision Labs",
            "descriptionText": "Design real-time object detection models with PyTorch and deploy with TensorRT."
            * 2,
            "jobUrl": "https://example.com/job/cv-1",
        }
    ]
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-retry",
            "actId": "act-retry",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-retry",
        },
    }
    request = _build_mock_request(payload)
    mock_settings = _make_test_settings(
        apify_api_token="dummy",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(
                return_value={
                    "id": "run-retry",
                    "actId": "act-retry",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "ds-retry",
                }
            ),
        ),
        patch("services.apify_receiver._fetch_dataset_items", new=AsyncMock(return_value=items)),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(1, 0))
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        # BQ deduplication returns empty list (all duplicate)
        mock_repo.batch_insert_jobs = AsyncMock(return_value=[])
        # In-band recovery finds that this job is still NEW_RAW in BQ
        mock_repo.get_unembedded_job_ids = AsyncMock(return_value=["job-cv-recovered"])

        response = await apify_webhook(request)

    assert response["inserted"] == 0
    assert response["recovered"] == 1
    assert response["published"] == 1
    assert response["publish_failures"] == 0
    mock_pub.assert_awaited_once_with(["job-cv-recovered"], project_id="test-p", topic="test-t")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconciliation_recovering_saved_but_unpublished_jobs():
    """reconcile_unpublished_jobs discovers saved NEW_RAW jobs and publishes them independently of the webhook."""
    mock_settings = _make_test_settings(
        apify_api_token="dummy",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )
    mock_repo = AsyncMock()
    mock_repo.get_unembedded_job_ids = AsyncMock(
        return_value=["saved-unembedded-1", "saved-unembedded-2"]
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch(
            "services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(2, 0))
        ) as mock_pub,
    ):
        result = await reconcile_unpublished_jobs(limit=50, min_age_seconds=60, repo=mock_repo)

    assert result["reconciled"] == 2
    assert result["published"] == 2
    assert result["publish_failures"] == 0
    assert result["job_ids"] == ["saved-unembedded-1", "saved-unembedded-2"]
    mock_repo.get_unembedded_job_ids.assert_awaited_once_with(
        job_ids=None, limit=50, min_age_seconds=60
    )
    mock_pub.assert_awaited_once_with(
        ["saved-unembedded-1", "saved-unembedded-2"],
        project_id="test-p",
        topic="test-t",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconciliation_when_no_unembedded_jobs():
    """reconcile_unpublished_jobs handles clean state when all jobs are already embedded."""
    mock_repo = AsyncMock()
    mock_repo.get_unembedded_job_ids = AsyncMock(return_value=[])

    with patch("services.apify_receiver._publish_job_ids") as mock_pub:
        result = await reconcile_unpublished_jobs(repo=mock_repo)

    assert result["reconciled"] == 0
    assert result["published"] == 0
    assert result["publish_failures"] == 0
    assert result["job_ids"] == []
    mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_dataset_items_sends_token_in_auth_header_and_not_in_url_or_params():
    """Verify that _fetch_dataset_items sends token in Authorization header, never in URL or params."""
    dataset_id = "test-dataset-123"
    api_token = "secret-apify-token-xyz-987"

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.is_success = True
    mock_response.json.return_value = [{"title": "AI Engineer", "jobUrl": "https://example.com/1"}]
    mock_client.get = AsyncMock(return_value=mock_response)

    items = await _fetch_dataset_items(dataset_id, api_token, clean=True, client=mock_client)

    assert len(items) == 1
    mock_client.get.assert_awaited_once()

    call_args, call_kwargs = mock_client.get.call_args
    request_url = call_args[0]
    headers = call_kwargs.get("headers", {})
    params = call_kwargs.get("params", {})

    # 1. Token sent in Authorization header as Bearer token
    assert headers.get("Authorization") == f"Bearer {api_token}"

    # 2. Token is NOT in URL or query parameters
    assert api_token not in request_url
    assert "token" not in params
    assert api_token not in str(params)

    # 3. format=json and clean=true remain intact
    assert params.get("format") == "json"
    assert params.get("clean") == "true"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_dataset_items_clean_false_preserves_optional_params():
    """Verify that clean=False preserves format=json and omits clean param without token in URL."""
    dataset_id = "test-dataset-456"
    api_token = "another-secret-token"

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.is_success = True
    mock_response.json.return_value = []
    mock_client.get = AsyncMock(return_value=mock_response)

    await _fetch_dataset_items(dataset_id, api_token, clean=False, client=mock_client)

    _, call_kwargs = mock_client.get.call_args
    headers = call_kwargs.get("headers", {})
    params = call_kwargs.get("params", {})

    assert headers.get("Authorization") == f"Bearer {api_token}"
    assert params == {"format": "json"}
    assert "token" not in params


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_dataset_items_handles_http_errors():
    """Verify handling of 401, 404, 500, and malformed JSON responses."""
    api_token = "test-token"
    mock_client = AsyncMock(spec=httpx.AsyncClient)

    # 401 Unauthorized
    resp_401 = MagicMock(spec=httpx.Response, status_code=401, is_success=False)
    mock_client.get = AsyncMock(return_value=resp_401)
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-1", api_token, client=mock_client)
    assert exc_info.value.status_code == 502
    assert "authentication failed" in exc_info.value.detail

    # 404 Not Found
    resp_404 = MagicMock(spec=httpx.Response, status_code=404, is_success=False)
    mock_client.get = AsyncMock(return_value=resp_404)
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-missing", api_token, client=mock_client)
    assert exc_info.value.status_code == 422
    assert "not found" in exc_info.value.detail

    # 500 Server Error
    resp_500 = MagicMock(spec=httpx.Response, status_code=500, is_success=False)
    mock_client.get = AsyncMock(return_value=resp_500)
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-500", api_token, client=mock_client)
    assert exc_info.value.status_code == 502
    assert "returned HTTP 500" in exc_info.value.detail

    # Malformed JSON
    resp_bad_json = MagicMock(spec=httpx.Response, status_code=200, is_success=True)
    resp_bad_json.json.side_effect = ValueError("Invalid JSON")
    mock_client.get = AsyncMock(return_value=resp_bad_json)
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-json", api_token, client=mock_client)
    assert exc_info.value.status_code == 502
    assert "non-JSON" in exc_info.value.detail


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_dataset_items_handles_timeouts_and_network_errors():
    """Verify timeout and network connection failure handling."""
    api_token = "test-token"
    mock_client = AsyncMock(spec=httpx.AsyncClient)

    # Timeout
    mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("Connection timed out"))
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-timeout", api_token, client=mock_client)
    assert exc_info.value.status_code == 504
    assert "timed out" in exc_info.value.detail

    # Request error / connection refused
    request = httpx.Request("GET", "https://api.apify.com/v2/datasets/ds-err/items?format=json")
    mock_client.get = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused", request=request)
    )
    with pytest.raises(HTTPException) as exc_info:
        await _fetch_dataset_items("ds-err", api_token, client=mock_client)
    assert exc_info.value.status_code == 502
    assert "Could not reach Apify Dataset API" in exc_info.value.detail


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_dataset_items_logs_and_errors_never_reveal_token(
    caplog: pytest.LogCaptureFixture,
):
    """Verify that API token is never exposed in logs or error messages under any scenario."""
    secret_token = "SUPER_SECRET_APIFY_TOKEN_9999"
    dataset_id = "ds-security-audit"
    mock_client = AsyncMock(spec=httpx.AsyncClient)

    scenarios = [
        # Scenario 1: 401 Auth Failure
        MagicMock(spec=httpx.Response, status_code=401, is_success=False),
        # Scenario 2: 404
        MagicMock(spec=httpx.Response, status_code=404, is_success=False),
        # Scenario 3: 502 non-2xx
        MagicMock(spec=httpx.Response, status_code=502, is_success=False),
    ]

    with caplog.at_level(logging.DEBUG):
        for resp in scenarios:
            mock_client.get = AsyncMock(return_value=resp)
            with pytest.raises(HTTPException) as exc_info:
                await _fetch_dataset_items(dataset_id, secret_token, client=mock_client)
            assert secret_token not in exc_info.value.detail

        # Scenario 4: Timeout exception
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("Read timeout"))
        with pytest.raises(HTTPException) as exc_info:
            await _fetch_dataset_items(dataset_id, secret_token, client=mock_client)
        assert secret_token not in exc_info.value.detail

        # Scenario 5: Request Error with URL in request
        request = httpx.Request(
            "GET",
            "https://api.apify.com/v2/datasets/ds-security-audit/items?format=json&clean=true",
            headers={"Authorization": f"Bearer {secret_token}"},
        )
        mock_client.get = AsyncMock(
            side_effect=httpx.ConnectError("Network is unreachable", request=request)
        )
        with pytest.raises(HTTPException) as exc_info:
            await _fetch_dataset_items(dataset_id, secret_token, client=mock_client)
        assert secret_token not in exc_info.value.detail

        # Scenario 6: Malformed JSON
        resp_bad_json = MagicMock(spec=httpx.Response, status_code=200, is_success=True)
        resp_bad_json.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
        mock_client.get = AsyncMock(return_value=resp_bad_json)
        with pytest.raises(HTTPException) as exc_info:
            await _fetch_dataset_items(dataset_id, secret_token, client=mock_client)
        assert secret_token not in exc_info.value.detail

    # Verify no log record ever contains the secret token across all logged events
    for record in caplog.records:
        assert secret_token not in record.message
        if record.exc_info:
            import traceback

            tb_str = "".join(traceback.format_exception(*record.exc_info))
            # If the ValueError mock deliberately contained the token, check that app logs didn't log it
            assert secret_token not in tb_str


# ── Webhook Trust Boundary & Security Tests (BUG 3) ───────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_missing_auth_configuration_in_production():
    """Missing APIFY_WEBHOOK_SECRET in production environment must fail with 500 error."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-1",
            "actId": "act-1",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-1",
        },
    }
    request = _build_mock_request(payload)
    prod_settings = _make_test_settings(
        environment="production",
        apify_webhook_secret="",  # Missing secret in production
        apify_api_token="valid-token",
        gcp_project_id="prod-project",
        pubsub_embedding_topic="prod-topic",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=prod_settings),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        with pytest.raises(HTTPException) as exc_info:
            await apify_webhook(request)

        assert exc_info.value.status_code == 500
        assert "not configured in production" in exc_info.value.detail
        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_missing_or_invalid_credentials():
    """Requests with missing or invalid HMAC signatures / secret headers must be rejected with 401."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-1",
            "actId": "act-1",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-1",
        },
    }
    settings_with_secret = _make_test_settings(
        environment="production",
        apify_webhook_secret="secure_hmac_secret_12345",
        apify_api_token="valid-token",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=settings_with_secret),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        # 1. Missing credentials header completely
        req_missing = _build_mock_request(payload)
        with pytest.raises(HTTPException) as exc1:
            await apify_webhook(req_missing)
        assert exc1.value.status_code == 401
        assert "Missing or invalid webhook credentials" in exc1.value.detail

        # 2. Invalid signature header
        req_bad_sig = _build_mock_request(
            payload,
            headers=[(b"x-apify-signature", b"invalid_hex_digest_999999999999")],
        )
        with pytest.raises(HTTPException) as exc2:
            await apify_webhook(req_bad_sig)
        assert exc2.value.status_code == 401
        assert "Missing or invalid webhook credentials" in exc2.value.detail

        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_malformed_json_and_payloads():
    """Malformed JSON or missing required fields must return 400 or 422 and halt execution."""
    settings = _make_test_settings(
        apify_api_token="valid-token",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        # 1. Non-JSON body -> 400
        req_non_json = _build_mock_request(b"<html>not json</html>")
        with pytest.raises(HTTPException) as exc1:
            await apify_webhook(req_non_json)
        assert exc1.value.status_code == 400

        # 2. JSON array instead of dict -> 400
        req_array = _build_mock_request(b"['item1', 'item2']")
        with pytest.raises(HTTPException) as exc2:
            await apify_webhook(req_array)
        assert exc2.value.status_code == 400

        # 3. Missing eventType -> 422
        req_no_event = _build_mock_request(
            {"resource": {"id": "1", "actId": "1", "defaultDatasetId": "ds"}}
        )
        with pytest.raises(HTTPException) as exc3:
            await apify_webhook(req_no_event)
        assert exc3.value.status_code == 422

        # 4. Missing resource object -> 422
        req_no_resource = _build_mock_request({"eventType": "ACTOR.RUN.SUCCEEDED"})
        with pytest.raises(HTTPException) as exc4:
            await apify_webhook(req_no_resource)
        assert exc4.value.status_code == 422

        # 5. Missing dataset ID -> 422
        req_no_ds = _build_mock_request(
            {
                "eventType": "ACTOR.RUN.SUCCEEDED",
                "resource": {
                    "id": "run-1",
                    "actId": "act-1",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "",
                },
            }
        )
        with pytest.raises(HTTPException) as exc5:
            await apify_webhook(req_no_ds)
        assert exc5.value.status_code == 422

        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_unexpected_event_types():
    """Non-SUCCEEDED event types are ignored without touching BQ or Pub/Sub."""
    payload = {
        "eventType": "ACTOR.RUN.FAILED",
        "resource": {
            "id": "run-1",
            "actId": "act-1",
            "status": "FAILED",
            "defaultDatasetId": "ds-1",
        },
    }
    request = _build_mock_request(payload)
    settings = _make_test_settings(
        apify_api_token="valid-token",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        response = await apify_webhook(request)

        assert response["received"] == "ACTOR.RUN.FAILED"
        assert response["inserted"] == 0
        assert response["published"] == 0
        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_failed_actor_runs():
    """Actor runs that did not succeed (e.g. status != SUCCEEDED) are skipped."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-1",
            "actId": "act-1",
            "status": "ABORTED",
            "defaultDatasetId": "ds-1",
        },
    }
    request = _build_mock_request(payload)
    settings = _make_test_settings(
        apify_api_token="valid-token",
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        response = await apify_webhook(request)

        assert response["received"] == "ABORTED"
        assert response["inserted"] == 0
        assert response["published"] == 0
        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_actor_ids_not_in_allowlist():
    """Actor IDs not present in apify_allowed_actor_ids must be rejected with 403 Forbidden."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-unauth",
            "actId": "unauthorized-actor-id",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-unauth",
        },
    }
    request = _build_mock_request(payload)
    settings = _make_test_settings(
        apify_api_token="valid-token",
        apify_allowed_actor_ids=["authorized-actor-1", "authorized-actor-2"],
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        with pytest.raises(HTTPException) as exc_info:
            await apify_webhook(request)

        assert exc_info.value.status_code == 403
        assert "not authorized" in exc_info.value.detail
        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_dataset_id_mismatch():
    """If payload dataset ID does not match the verified Apify run's dataset ID, reject with 422."""
    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-spoofed",
            "actId": "auth-actor-1",
            "status": "SUCCEEDED",
            "defaultDatasetId": "attacker-claimed-dataset-id",
        },
    }
    request = _build_mock_request(payload)
    settings = _make_test_settings(
        apify_api_token="valid-token",
        apify_allowed_actor_ids=["auth-actor-1"],
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    verified_run_data = {
        "id": "run-spoofed",
        "actId": "auth-actor-1",
        "status": "SUCCEEDED",
        "defaultDatasetId": "actual-real-dataset-id-from-apify",
    }

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified_run_data),
        ),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        with pytest.raises(HTTPException) as exc_info:
            await apify_webhook(request)

        assert exc_info.value.status_code == 422
        assert "Dataset ID mismatch" in exc_info.value.detail
        mock_fetch.assert_not_called()
        mock_repo_cls.assert_not_called()
        mock_pub.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_webhook_valid_authenticated_and_verified_run():
    """Legitimate webhook with valid HMAC signature and verified run succeeds end-to-end."""
    secret = "production_super_secret_webhook_key_777"
    raw_payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": "run-legit-123",
            "actId": "verified-career-scraper",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-legit-123",
        },
    }
    body_bytes = json.dumps(raw_payload).encode("utf-8")
    valid_sig = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()

    request = _build_mock_request(
        raw_payload,
        headers=[(b"x-apify-signature", valid_sig.encode("utf-8"))],
    )

    settings = _make_test_settings(
        environment="production",
        apify_webhook_secret=secret,
        apify_api_token="valid-apify-token",
        apify_allowed_actor_ids=["verified-career-scraper"],
        gcp_project_id="test-p",
        pubsub_embedding_topic="test-t",
    )

    verified_run_data = {
        "id": "run-legit-123",
        "actId": "verified-career-scraper",
        "status": "SUCCEEDED",
        "defaultDatasetId": "ds-legit-123",
    }
    sample_dataset = [
        {
            "title": "Machine Learning Engineer",
            "companyName": "TechCorp",
            "descriptionText": "Develop advanced deep learning models and agentic workflows." * 2,
            "jobUrl": "https://example.com/job/valid-1",
        }
    ]

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified_run_data),
        ) as mock_verify,
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=sample_dataset),
        ) as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(1, 0))
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["job-valid-1"])

        response = await apify_webhook(request)

        assert response["dataset_id"] == "ds-legit-123"
        assert response["inserted"] == 1
        assert response["published"] == 1
        assert response["publish_failures"] == 0

        mock_verify.assert_awaited_once_with("run-legit-123", "valid-apify-token")
        mock_fetch.assert_awaited_once_with("ds-legit-123", "valid-apify-token")
        mock_repo.batch_insert_jobs.assert_awaited_once()
        mock_pub.assert_awaited_once_with(["job-valid-1"], project_id="test-p", topic="test-t")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_verify_actor_run_helper():
    """Verify _verify_actor_run error handling and response parsing with mock client."""
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    token = "test-token"

    # 1. Successful run lookup
    resp_200 = MagicMock(spec=httpx.Response, status_code=200, is_success=True)
    resp_200.json.return_value = {
        "data": {
            "id": "run-ok",
            "actId": "actor-ok",
            "status": "SUCCEEDED",
            "defaultDatasetId": "ds-ok",
        }
    }
    mock_client.get = AsyncMock(return_value=resp_200)
    data = await _verify_actor_run("run-ok", token, client=mock_client)
    assert data["id"] == "run-ok"
    assert data["status"] == "SUCCEEDED"

    # 2. 404 Run Not Found
    resp_404 = MagicMock(spec=httpx.Response, status_code=404, is_success=False)
    mock_client.get = AsyncMock(return_value=resp_404)
    with pytest.raises(HTTPException) as exc1:
        await _verify_actor_run("run-missing", token, client=mock_client)
    assert exc1.value.status_code == 404

    # 3. 500 Apify Server Error
    resp_500 = MagicMock(spec=httpx.Response, status_code=500, is_success=False)
    mock_client.get = AsyncMock(return_value=resp_500)
    with pytest.raises(HTTPException) as exc2:
        await _verify_actor_run("run-err", token, client=mock_client)
    assert exc2.value.status_code == 502

    # 4. Timeout
    mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
    with pytest.raises(HTTPException) as exc3:
        await _verify_actor_run("run-time", token, client=mock_client)
    assert exc3.value.status_code == 504

    # 5. ConnectError
    req = httpx.Request("GET", "https://api.apify.com/v2/actor-runs/run-conn")
    mock_client.get = AsyncMock(side_effect=httpx.ConnectError("Conn error", request=req))
    with pytest.raises(HTTPException) as exc4:
        await _verify_actor_run("run-conn", token, client=mock_client)
    assert exc4.value.status_code == 502


@pytest.fixture
def group1_ingestion(monkeypatch: pytest.MonkeyPatch):
    """Run real normalization and batching, with every external operation replaced."""
    from services import apify_receiver as receiver

    resource = {
        "id": "run-group1", "actId": "actor-group1", "status": "SUCCEEDED",
        "defaultDatasetId": "dataset-group1",
    }
    settings = _make_test_settings()
    fetch = AsyncMock()
    repo = MagicMock()
    repo.batch_insert_jobs = AsyncMock(side_effect=lambda jobs: [job.job_id for job in jobs])
    repo.get_unembedded_job_ids = AsyncMock(return_value=[])
    publish = AsyncMock(side_effect=lambda ids, **kwargs: (len(ids), 0))
    monkeypatch.setattr(receiver, "get_settings", lambda: settings)
    monkeypatch.setattr(receiver, "_verify_actor_run", AsyncMock(return_value=resource))
    monkeypatch.setattr(receiver, "_fetch_dataset_items", fetch)
    monkeypatch.setattr(receiver, "RawJobRepository", lambda: repo)
    monkeypatch.setattr(receiver, "_publish_job_ids", publish)
    payload = {"eventType": "ACTOR.RUN.SUCCEEDED", "resource": resource}
    return payload, fetch, repo, publish


@pytest.fixture
def group1_dataset() -> list[dict[str, Any]]:
    dataset_path = (
        Path(__file__).resolve().parents[2]
        / "Input/dataset_company-career-page-scraper_2026-09-17_06-15-27-909.json"
    )
    # This regression requires the downloaded fixture; never silently skip it.
    return json.loads(dataset_path.read_text(encoding="utf-8-sig"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_group1_full_fixture_counts_and_order(group1_ingestion, group1_dataset):
    from services.normalizers.apify_career_scraper import normalize

    payload, fetch, repo, publish = group1_ingestion
    original = deepcopy(group1_dataset)
    fetch.return_value = group1_dataset
    response = await apify_webhook(_build_mock_request(payload))

    valid = [job for item in group1_dataset if (job := normalize(item)) is not None]
    first_jobs = {}
    for job in valid:
        first_jobs.setdefault(job.job_id, job)
    assert len(group1_dataset) == 215
    assert len(valid) == 215
    assert len(first_jobs) == 113
    assert response == {
        "dataset_id": "dataset-group1", "actor_run_id": "run-group1",
        "received": 215, "normalized": 215, "rejected": 0,
        "unique": 113, "in_batch_duplicates": 102, "apply_url_fallback": 89,
        "inserted": 113, "published": 113, "publish_failures": 0, "recovered": 0,
    }
    submitted = repo.batch_insert_jobs.await_args.args[0]
    assert [job.job_id for job in submitted] == list(first_jobs)
    # Compare source-derived content, excluding the ingestion timestamp.
    assert [job.model_dump(exclude={"scraped_at"}) for job in submitted] == [
        job.model_dump(exclude={"scraped_at"}) for job in first_jobs.values()
    ]
    publish.assert_awaited_once_with(
        list(first_jobs), project_id="test-project", topic="test-topic",
    )
    repo.get_unembedded_job_ids.assert_not_awaited()
    assert group1_dataset == original


@pytest.mark.unit
@pytest.mark.asyncio
async def test_group1_full_fixture_malformed_then_valid_and_duplicates(
    group1_ingestion, group1_dataset,
):
    from services.normalizers.apify_career_scraper import normalize

    payload, fetch, repo, publish = group1_ingestion
    first = group1_dataset[0]
    # The invalid first occurrence must not reserve the canonical ID.
    malformed = [
        None, [], "not-an-object", 42,
        {**first, "title": 123},
        {**first, "jobId": {}},
        {**first, "jobUrl": ["https://example.com/job"]},
    ]
    duplicate = {
        **first, "jobId": f"  {first['jobId']}  ",
        "title": "A later valid title that must not replace the first valid job",
    }
    nullable = {
        **first, "jobId": "group1-null-optionals",
        "location": None, "salary": None, "source": None,
        "employmentType": None, "postedAt": {"invalid": "date"},
    }
    fetch.return_value = [*malformed, *group1_dataset, duplicate, nullable]
    response = await apify_webhook(_build_mock_request(payload))

    assert response["received"] == 224
    assert response["normalized"] == 217
    assert response["rejected"] == 7
    assert response["unique"] == 114
    assert response["in_batch_duplicates"] == 103
    assert response["received"] == response["normalized"] + response["rejected"]
    assert response["normalized"] == response["unique"] + response["in_batch_duplicates"]
    submitted = repo.batch_insert_jobs.await_args.args[0]
    expected_first = normalize(first)
    assert expected_first is not None
    assert submitted[0].model_dump(exclude={"scraped_at"}) == expected_first.model_dump(
        exclude={"scraped_at"}
    )
    assert submitted[-1].job_id == "group1-null-optionals"
    assert submitted[-1].location is None
    assert submitted[-1].salary_raw is None
    assert submitted[-1].posted_at is None
    ids = [job.job_id for job in submitted]
    assert len(ids) == len(set(ids)) == 114
    publish.assert_awaited_once_with(ids, project_id="test-project", topic="test-topic")


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("items", [[], [None, [], {"title": "missing required fields"}]])
async def test_group1_empty_or_all_rejected_counters(group1_ingestion, items):
    payload, fetch, repo, publish = group1_ingestion
    fetch.return_value = items
    response = await apify_webhook(_build_mock_request(payload))
    assert response["received"] == response["rejected"] == len(items)
    assert response["normalized"] == response["unique"] == response["in_batch_duplicates"] == 0
    assert response["inserted"] == response["published"] == 0
    assert response["dataset_id"] == "dataset-group1"
    if items:
        assert response["apply_url_fallback"] == 0
        assert response["message"] == "All items were malformed or had insufficient description"
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("exception_type", [TypeError, ValueError])
@pytest.mark.parametrize("operation", ["normalize", "resolve_job_url"])
async def test_group1_receiver_isolates_record_errors(
    group1_ingestion, mock_dataset_items, monkeypatch: pytest.MonkeyPatch,
    exception_type, operation: str,
):
    from services.normalizers import apify_career_scraper

    payload, fetch, repo, publish = group1_ingestion
    bad = {"jobId": "bad-record"}
    valid = mock_dataset_items[0]
    fetch.return_value = [bad, valid]
    original = getattr(apify_career_scraper, operation)

    def fail_one(item):
        if item is bad:
            raise exception_type("malformed source record")
        return original(item)

    monkeypatch.setattr(apify_career_scraper, operation, fail_one)
    response = await apify_webhook(_build_mock_request(payload))
    assert response["received"] == 2
    assert response["normalized"] == response["unique"] == 1
    assert response["rejected"] == 1
    assert response["in_batch_duplicates"] == 0
    assert len(repo.batch_insert_jobs.await_args.args[0]) == 1
    publish.assert_awaited_once()
