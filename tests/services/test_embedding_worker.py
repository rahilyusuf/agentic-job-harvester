"""tests/services/test_embedding_worker.py — Tests for embedding worker push processing and idempotency."""

from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request

from schemas.job_models import JobStatus, RawJob
from services.embedding_worker import pubsub_push, _process_job


def _build_pubsub_request(job_id: str) -> Request:
    data_bytes = json.dumps({"job_id": job_id}).encode("utf-8")
    b64_data = base64.b64encode(data_bytes).decode("utf-8")
    envelope = {"message": {"data": b64_data, "messageId": "msg-123"}}
    body = json.dumps(envelope).encode("utf-8")

    async def receive() -> dict[str, str | bytes | bool]:
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"content-type", b"application/json")],
    }
    return Request(scope, receive)


def _make_dummy_job(job_id: str, status: JobStatus = JobStatus.NEW_RAW) -> RawJob:
    return RawJob(
        job_id=job_id,
        source="test",
        job_url="https://example.com/job/1",  # type: ignore[arg-type]
        title="AI Engineer",
        company="TechCorp",
        location="Dubai",
        description="Build LLM applications with python, langchain and embeddings for enterprise." * 2,
        status=status,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_duplicate_pubsub_delivery():
    """Duplicate Pub/Sub push messages are skipped idempotently when job is already embedded."""
    job_id = "job-dup-123"
    request = _build_pubsub_request(job_id)
    already_done_job = _make_dummy_job(job_id, status=JobStatus.EMBEDDING_DONE)

    with (
        patch("services.embedding_worker.RawJobRepository") as mock_raw_cls,
        patch("services.embedding_worker.EmbeddingRepository") as mock_emb_cls,
        patch("services.embedding_worker.generate_embedding") as mock_generate_emb,
    ):
        mock_raw = mock_raw_cls.return_value
        mock_raw.get_job = AsyncMock(return_value=already_done_job)
        mock_raw.update_status = AsyncMock()
        mock_emb = mock_emb_cls.return_value
        mock_emb.upsert_embedding = AsyncMock()

        await pubsub_push(request)

    mock_raw.get_job.assert_awaited_once_with(job_id)
    # Embedding generation and upsert must NOT be invoked on duplicate delivery
    mock_generate_emb.assert_not_called()
    mock_emb.upsert_embedding.assert_not_called()
    mock_raw.update_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_crash_and_retry():
    """Worker failure resets status back to NEW_RAW so reconciliation or redelivery can retry."""
    job_id = "job-crash-456"
    request = _build_pubsub_request(job_id)
    raw_job = _make_dummy_job(job_id, status=JobStatus.NEW_RAW)

    with (
        patch("services.embedding_worker.RawJobRepository") as mock_raw_cls,
        patch("services.embedding_worker.EmbeddingRepository") as mock_emb_cls,
        patch("services.embedding_worker.generate_embedding", side_effect=RuntimeError("LiteLLM proxy timeout")),
    ):
        mock_raw = mock_raw_cls.return_value
        mock_raw.get_job = AsyncMock(return_value=raw_job)
        mock_raw.update_status = AsyncMock()
        mock_emb = mock_emb_cls.return_value
        mock_emb.upsert_embedding = AsyncMock()

        with pytest.raises(RuntimeError, match="LiteLLM proxy timeout"):
            await pubsub_push(request)

    # 1. Marked as EMBEDDING_QUEUED
    # 2. Reset back to NEW_RAW upon failure
    assert mock_raw.update_status.await_count == 2
    mock_raw.update_status.assert_has_awaits([
        ((job_id, JobStatus.EMBEDDING_QUEUED.value),),
        ((job_id, JobStatus.NEW_RAW.value),),
    ])
    mock_emb.upsert_embedding.assert_not_called()
