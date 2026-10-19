"""tests/services/test_apify_receiver.py — Integration and stage flow tests for apify_receiver."""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request

from config.settings import Settings
from services.apify_receiver import apify_webhook


def _build_mock_request(payload: dict[str, Any]) -> Request:
    body = json.dumps(payload).encode("utf-8")

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/webhook/apify",
        "headers": [(b"content-type", b"application/json")],
    }
    return Request(scope, receive)


@pytest.fixture
def mock_dataset_items() -> list[dict[str, Any]]:
    return [
        # Item 1: Direct jobUrl
        {
            "title": "Data Engineer 1",
            "companyName": "Company A",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
            "jobUrl": "https://example.com/job1",
            "applyUrl": "https://example.com/apply1",
        },
        # Item 2: Direct jobUrl (no applyUrl)
        {
            "title": "Data Engineer 2",
            "companyName": "Company B",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
            "jobUrl": "https://example.com/job2",
            "applyUrl": "",
        },
        # Item 3: Empty jobUrl, valid applyUrl -> fallback 1
        {
            "title": "Workday Engineer 1",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
            "jobUrl": "",
            "applyUrl": "https://workday.example.com/job3",
        },
        # Item 4: Whitespace jobUrl, valid applyUrl -> fallback 2
        {
            "title": "Workday Engineer 2",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
            "jobUrl": "   ",
            "applyUrl": "https://workday.example.com/job4",
        },
        # Item 5: None jobUrl, valid applyUrl -> fallback 3
        {
            "title": "Workday Engineer 3",
            "companyName": "Workday Inc",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
            "jobUrl": None,
            "applyUrl": "https://workday.example.com/job5",
        },
        # Item 6: Both missing -> skipped
        {
            "title": "Invalid Job",
            "companyName": "Nowhere",
            "descriptionText": "Valid job description content that meets the minimum length requirement easily." * 2,
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

    mock_settings = Settings(
        apify_webhook_secret="",  # disable signature check
        apify_api_token="dummy-token",
        gcp_project_id="test-project",
        pubsub_embedding_topic="test-topic",
    )

    with (
        patch("services.apify_receiver.get_settings", return_value=mock_settings),
        patch("services.apify_receiver._fetch_dataset_items", new=AsyncMock(return_value=mock_dataset_items)),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids", new=AsyncMock(return_value=(5, 0))),
        caplog.at_level(logging.INFO, logger="services.apify_receiver"),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(return_value=["id1", "id2", "id3", "id4", "id5"])

        response = await apify_webhook(request)

    # 1. Assert response structure & counts
    assert response["dataset_id"] == "dataset-xyz"
    assert response["received"] == 6
    assert response["normalized"] == 5
    assert response["apply_url_fallback"] == 3
    assert response["inserted"] == 5
    assert response["published"] == 5

    # 2. Assert stage log flow
    log_text = "\n".join(record.message for record in caplog.records)
    assert "── APIFY WEBHOOK START" in log_text
    assert "── WEBHOOK PARSED" in log_text
    assert "── FETCHING DATASET  dataset_id=dataset-xyz" in log_text
    assert "── DATASET FETCHED  item_count=6  dataset_id=dataset-xyz" in log_text
    assert "── NORMALIZATION START  items=6" in log_text
    assert "── NORMALIZATION COMPLETE  received=6  valid=5  skipped=1  apply_url_fallback=3" in log_text
    assert "── BIGQUERY PERSIST START  jobs_to_insert=5" in log_text
    assert "── BIGQUERY PERSIST COMPLETE  novel_inserted=5  duplicates_skipped=0" in log_text
    assert "── PUBSUB PUBLISH START  job_ids_to_publish=5" in log_text
    assert "── PUBSUB PUBLISH COMPLETE  published=5  failures=0" in log_text
    assert "── APIFY WEBHOOK COMPLETE  dataset_id=dataset-xyz  received=6  normalized=5  apply_url_fallback=3" in log_text
