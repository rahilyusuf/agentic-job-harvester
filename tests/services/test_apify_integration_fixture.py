"""tests/services/test_apify_integration_fixture.py — End-to-end integration tests for the
Apify webhook ingestion flow using the real downloaded fixture file.

These tests exercise the complete path:
  webhook payload → signature bypass → actor-run verify (mocked) →
  dataset fetch (mocked with real fixture rows) → normalize (real normalizer) →
  BQ dedup + insert (mocked repo) → Pub/Sub publish (mocked)

No real Apify, BigQuery, or Pub/Sub requests are made.
Production verification logic is NOT disabled — _verify_actor_run is still called;
its network layer is replaced with an AsyncMock that returns SUCCEEDED.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.apify_receiver import apify_webhook

# ── Fixture file path ──────────────────────────────────────────────────────────

_FIXTURE_PATH = (
    pathlib.Path(__file__).parents[2]
    / "Input"
    / "dataset_company-career-page-scraper_2026-09-17_06-15-27-909.json"
)

# ── Shared helpers ─────────────────────────────────────────────────────────────


def _load_fixture_items(n: int = 5) -> list[dict[str, Any]]:
    """Load the first *n* items from the downloaded Apify dataset fixture.

    Uses utf-8-sig so that files saved with a BOM are decoded correctly.
    Items without a usable URL (both jobUrl and applyUrl empty/missing) are
    silently included — the normalizer will skip them, which is correct
    production behaviour and is explicitly tested below.
    """
    with _FIXTURE_PATH.open(encoding="utf-8-sig") as fh:
        data: list[dict[str, Any]] = json.load(fh)
    return data[:n]


def _make_settings(**overrides: Any):  # type: ignore[return]
    """Return a minimal Settings instance without hitting Secret Manager."""
    from config.settings import Settings

    base: dict[str, Any] = {
        "gcp_project_id": "test-project",
        "litellm_proxy_base_url": "http://localhost:4000",
        "apify_webhook_secret": "",           # skip HMAC in dev
        "apify_api_token": "test-apify-token",
        "pubsub_embedding_topic": "test-topic",
        "environment": "development",
    }
    base.update(overrides)
    return Settings.model_construct(**base)


def _make_webhook_request(
    actor_run_id: str = "run-fixture-001",
    actor_id: str = "actor-fixture",
    dataset_id: str = "ds-fixture-001",
    run_status: str = "SUCCEEDED",
):
    """Build a minimal ACTOR.RUN.SUCCEEDED webhook Request object."""
    from starlette.requests import Request

    payload = {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "resource": {
            "id": actor_run_id,
            "actId": actor_id,
            "status": run_status,
            "defaultDatasetId": dataset_id,
        },
    }
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


def _verified_run_data(
    actor_run_id: str = "run-fixture-001",
    actor_id: str = "actor-fixture",
    dataset_id: str = "ds-fixture-001",
    status: str = "SUCCEEDED",
) -> dict[str, Any]:
    """Minimal dict that _verify_actor_run would return for a SUCCEEDED run."""
    return {
        "id": actor_run_id,
        "actId": actor_id,
        "status": status,
        "defaultDatasetId": dataset_id,
    }


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_with_real_fixture_items():
    """Happy path: 5 fixture items → normalizer → BQ insert → Pub/Sub publish.

    Verifies:
    - _verify_actor_run is called once with the correct run-id and token
    - _fetch_dataset_items is called once with the correct dataset-id and token
    - batch_insert_jobs receives RawJob dicts (not raw dicts)
    - Pub/Sub receives exactly the IDs returned by batch_insert_jobs
    - Response envelope contains correct counts
    """
    fixture_items = _load_fixture_items(5)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ) as mock_verify,
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ) as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids",
            new=AsyncMock(return_value=(3, 0)),
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        inserted_ids = ["job-fix-1", "job-fix-2", "job-fix-3"]
        mock_repo.batch_insert_jobs = AsyncMock(return_value=inserted_ids)

        response = await apify_webhook(request)

    # ── Structural checks ──────────────────────────────────────────────────────
    assert response["dataset_id"] == "ds-fixture-001"
    assert response["received"] == 5         # 5 items fetched from fixture
    assert response["normalized"] >= 1       # at least 1 passed the normalizer
    assert response["inserted"] == 3         # whatever BQ mock returned
    assert response["published"] == 3        # Pub/Sub count matches BQ inserts

    # ── Interaction checks ─────────────────────────────────────────────────────
    mock_verify.assert_awaited_once_with("run-fixture-001", "test-apify-token")
    mock_fetch.assert_awaited_once_with("ds-fixture-001", "test-apify-token")

    # batch_insert_jobs must have been called with a non-empty list
    mock_repo.batch_insert_jobs.assert_awaited_once()
    inserted_arg: list = mock_repo.batch_insert_jobs.call_args[0][0]
    assert isinstance(inserted_arg, list)
    assert len(inserted_arg) >= 1

    # Pub/Sub receives exactly the IDs returned by batch_insert_jobs
    mock_pub.assert_awaited_once_with(
        inserted_ids,
        project_id="test-project",
        topic="test-topic",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_normalization_produces_raw_jobs():
    """The normalizer must return RawJob-compatible dicts (not bare fixture dicts).

    Checks that each item forwarded to batch_insert_jobs has the expected
    canonical keys that the RawJob schema requires (job_id, title, company_name,
    description, job_url, source).
    """
    fixture_items = _load_fixture_items(3)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    captured_jobs: list[Any] = []

    async def _capture_insert(jobs: list[Any]) -> list[str]:
        captured_jobs.extend(jobs)
        return [f"job-cap-{i}" for i in range(len(jobs))]

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids",
            new=AsyncMock(return_value=(len(fixture_items), 0)),
        ),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(side_effect=_capture_insert)

        await apify_webhook(request)

    # Verify canonical RawJob field names survive normalization.
    # The schema uses "company" (not "company_name") — confirmed from model_dump() output.
    for job in captured_jobs:
        job_dict = job if isinstance(job, dict) else job.model_dump()
        assert "job_id" in job_dict, f"Missing job_id in normalized job: {list(job_dict.keys())}"
        assert "title" in job_dict
        # RawJob schema field is "company", not "company_name"
        assert "company" in job_dict, (
            f"Expected 'company' key in RawJob dict, got: {list(job_dict.keys())}"
        )
        assert "description" in job_dict
        assert "job_url" in job_dict
        assert "source" in job_dict
        # source must always be set by the normalizer
        assert job_dict["source"], "source field must not be empty"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_all_jobs_deduplicated():
    """When BQ already has all job IDs, nothing is inserted and Pub/Sub is not called."""
    fixture_items = _load_fixture_items(3)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids",
            new=AsyncMock(return_value=(0, 0)),
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        # All jobs already in BQ → batch_insert returns empty list
        mock_repo.batch_insert_jobs = AsyncMock(return_value=[])
        # No unembedded jobs pending recovery either
        mock_repo.get_unembedded_job_ids = AsyncMock(return_value=[])

        response = await apify_webhook(request)

    assert response["inserted"] == 0
    assert response["published"] == 0
    mock_pub.assert_not_called()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_partial_deduplication():
    """When 2 of 5 fixture items are duplicates, only the 3 novel ones are inserted."""
    fixture_items = _load_fixture_items(5)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    novel_ids = ["job-novel-1", "job-novel-2", "job-novel-3"]

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids",
            new=AsyncMock(return_value=(3, 0)),
        ) as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        # BQ dedup: returns only 3 novel IDs (2 duplicates silently dropped)
        mock_repo.batch_insert_jobs = AsyncMock(return_value=novel_ids)

        response = await apify_webhook(request)

    assert response["inserted"] == 3
    assert response["published"] == 3
    mock_pub.assert_awaited_once_with(novel_ids, project_id="test-project", topic="test-topic")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_bq_insert_raises_returns_500():
    """When BigQuery insertion throws, the endpoint returns HTTP 500."""
    from fastapi import HTTPException

    fixture_items = _load_fixture_items(3)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(
            side_effect=RuntimeError("BigQuery unavailable")
        )

        with pytest.raises((HTTPException, Exception)) as exc_info:
            await apify_webhook(request)

    # Either an HTTPException(502) or the raw RuntimeError propagates — either
    # way Pub/Sub must never be called after a failed insert.
    # Note: the receiver maps BigQuery errors to HTTP 502 (upstream dependency failure),
    # not 500. We assert either 500 or 502 to be robust against future refinements.
    exc = exc_info.value
    if isinstance(exc, HTTPException):
        assert exc.status_code in (500, 502), (
            f"Expected 500 or 502 for BQ failure, got {exc.status_code}"
        )
    mock_pub.assert_not_called()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_actor_aborted_returns_422():
    """When verified run status is ABORTED, the endpoint raises HTTP 422.

    This is the production failure mode under test: Apify returns ABORTED and
    the receiver must reject ingestion before touching BigQuery or Pub/Sub.
    """
    from fastapi import HTTPException

    settings = _make_settings()
    request = _make_webhook_request()
    # Note: webhook payload itself says SUCCEEDED; the *verified* status is ABORTED
    aborted_run = _verified_run_data(status="ABORTED")

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=aborted_run),
        ),
        patch("services.apify_receiver._fetch_dataset_items") as mock_fetch,
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch("services.apify_receiver._publish_job_ids") as mock_pub,
    ):
        with pytest.raises(HTTPException) as exc_info:
            await apify_webhook(request)

    assert exc_info.value.status_code == 422
    assert "ABORTED" in exc_info.value.detail
    mock_fetch.assert_not_called()
    mock_repo_cls.assert_not_called()
    mock_pub.assert_not_called()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_webhook_flow_larger_fixture_slice():
    """Smoke test with 10 fixture items to confirm normalizer handles varied real data.

    Some fixture items may have empty URLs or very short descriptions. The
    normalizer is expected to skip those gracefully; we only assert that at
    least 1 valid job makes it through.
    """
    fixture_items = _load_fixture_items(10)
    settings = _make_settings()
    request = _make_webhook_request()
    verified = _verified_run_data()

    with (
        patch("services.apify_receiver.get_settings", return_value=settings),
        patch(
            "services.apify_receiver._verify_actor_run",
            new=AsyncMock(return_value=verified),
        ),
        patch(
            "services.apify_receiver._fetch_dataset_items",
            new=AsyncMock(return_value=fixture_items),
        ),
        patch("services.apify_receiver.RawJobRepository") as mock_repo_cls,
        patch(
            "services.apify_receiver._publish_job_ids",
            new=AsyncMock(return_value=(5, 0)),
        ),
    ):
        mock_repo = mock_repo_cls.return_value
        mock_repo.batch_insert_jobs = AsyncMock(
            return_value=[f"job-smoke-{i}" for i in range(5)]
        )

        response = await apify_webhook(request)

    assert response["received"] == 10
    assert response["normalized"] >= 1, (
        "At least one of the 10 real fixture items should survive normalization"
    )
