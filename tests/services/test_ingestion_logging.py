"""Semantic logging regressions with mocked transport/persistence and synthetic secrets."""

import asyncio
import io
import json
import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from observability.ingestion_logging import (
    configure_ingestion_logging,
    context,
    emit,
    request_context,
    stop_ingestion_logging,
)
from services import apify_receiver as receiver
from tests.services.test_apify_receiver import _build_mock_request, _make_test_settings


@pytest.fixture
def ingestion(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ingestion.events")
    resource = {
        "id": "run-logging",
        "actId": "actor-logging",
        "status": "SUCCEEDED",
        "defaultDatasetId": "dataset-logging",
    }
    item = {
        "jobId": "job-logging",
        "title": "ML Engineer",
        "companyName": "Example",
        "jobUrl": "https://example.com/jobs/1",
        "descriptionText": "A sufficiently long public description for an engineering role. " * 2,
    }
    repo = MagicMock()
    repo.batch_insert_jobs = AsyncMock(side_effect=lambda jobs: [job.job_id for job in jobs])
    repo.get_unembedded_job_ids = AsyncMock(return_value=[])
    verify = AsyncMock(return_value=dict(resource))
    fetch = AsyncMock(return_value=[item])
    publish = AsyncMock(return_value=(1, 0))
    monkeypatch.setattr(receiver, "get_settings", lambda: _make_test_settings())
    monkeypatch.setattr(receiver, "RawJobRepository", lambda: repo)
    monkeypatch.setattr(receiver, "_verify_actor_run", verify)
    monkeypatch.setattr(receiver, "_fetch_dataset_items", fetch)
    monkeypatch.setattr(receiver, "_publish_job_ids", publish)
    return resource, item, repo, verify, fetch, publish


def events(caplog):
    return [record.event_fields for record in caplog.records if hasattr(record, "event_fields")]


async def invoke(ingestion, header=None):
    resource = ingestion[0]
    headers = [(b"x-request-id", header.encode())] if header else None
    return await receiver.apify_webhook(
        _build_mock_request(
            {"eventType": "ACTOR.RUN.SUCCEEDED", "resource": resource},
            headers=headers,
        )
    )


@pytest.mark.asyncio
async def test_success_summary_and_stage_durations(ingestion, caplog):
    result = await invoke(ingestion)
    records = events(caplog)
    final = records[-1]
    assert final["event"] == "ingestion.completed" and final["outcome"] == "success"
    for key in (
        "received",
        "normalized",
        "rejected",
        "unique",
        "in_batch_duplicates",
        "apply_url_fallback",
        "inserted",
        "recovered",
        "published",
        "publish_failures",
    ):
        assert final[key] == result[key]
    assert all(record["duration_ms"] >= 0 for record in records)
    assert len({record["request_id"] for record in records}) == 1
    assert final["subscriber_processing"] == "not_observed"
    assert context() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,stage",
    [("verify", "actor_verification"), ("fetch", "dataset_fetch"), ("insert", "bigquery")],
)
async def test_failures_identify_stage_and_do_not_claim_success(
    ingestion, caplog, operation, stage
):
    _, _, repo, verify, fetch, publish = ingestion
    target = {"verify": verify, "fetch": fetch, "insert": repo.batch_insert_jobs}[operation]
    target.side_effect = TimeoutError("sensitive upstream exception")
    with pytest.raises((TimeoutError, receiver.HTTPException)):
        await invoke(ingestion)
    records = events(caplog)
    final = records[-1]
    assert final["event"] == "ingestion.failed" and final["stage"] == stage
    assert final["severity"] == "ERROR"
    assert final["error_type"] == "TimeoutError"
    assert "error_location" in final
    assert not any(record["event"] == "ingestion.completed" for record in records)
    assert "sensitive upstream exception" not in caplog.text
    if operation == "insert":
        assert final["persistence_outcome"] == "unconfirmed"
        assert final["inserted"] is None  # No claim that zero/all rows were stored.
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("published,outcome", [(0, "publication_unconfirmed"), (1, "partial")])
async def test_partial_publication_is_warning_not_full_success(
    ingestion, caplog, published, outcome
):
    if published:
        ingestion[4].return_value.append({**ingestion[1], "jobId": "second-job"})
    ingestion[-1].return_value = (published, 1)
    response = await invoke(ingestion)
    assert response["publish_failures"] == 1
    final = events(caplog)[-1]
    assert final["severity"] == "WARNING"
    assert final["outcome"] == outcome
    assert final["published"] == published and final["publish_failures"] == 1


@pytest.mark.asyncio
async def test_malformed_and_duplicate_counts_are_aggregated(ingestion, caplog):
    resource, item, _, _, fetch, _ = ingestion
    fetch.return_value = [None, item, {**item, "title": "Later duplicate"}, {"jobId": "bad"}]
    await invoke(ingestion)
    final = events(caplog)[-1]
    assert final["received"] == 4 and final["normalized"] == 2
    assert final["rejected"] == 2 and final["unique"] == 1
    assert final["in_batch_duplicates"] == 1
    assert final["outcome"] == "completed_with_rejections"
    diagnostics = [r for r in events(caplog) if r["event"] == "ingestion.record.rejected"]
    assert {r["record_index"] for r in diagnostics} == {0, 3}
    assert all(r["severity"] == "DEBUG" for r in diagnostics)


@pytest.mark.asyncio
async def test_recovery_failure_is_not_logged_as_full_success(ingestion, caplog):
    repo = ingestion[2]
    repo.batch_insert_jobs.side_effect = None
    repo.batch_insert_jobs.return_value = []
    repo.get_unembedded_job_ids.side_effect = RuntimeError("private recovery details")
    response = await invoke(ingestion)
    assert response["recovered"] == 0  # Existing API behavior is retained.
    final = events(caplog)[-1]
    assert final["severity"] == "WARNING" and final["outcome"] == "partial"
    assert "private recovery details" not in caplog.text


@pytest.mark.asyncio
async def test_secret_payload_url_and_retry_messages_are_not_logged(ingestion, caplog, monkeypatch):
    secret = "SENSITIVE_TOKEN_CANARY_123456"
    resource, item, repo, _, fetch, _ = ingestion
    item.update(title=secret, descriptionText=secret * 10, jobUrl=f"https://example.com/{secret}")
    resource["id"] = secret
    monkeypatch.setattr(
        receiver, "get_settings", lambda: _make_test_settings(apify_api_token=secret)
    )

    async def fail(jobs):
        logging.getLogger("utils.retry").warning(
            "%s attempt %d/%d failed (%s)", "insert", 1, 3, secret
        )
        logging.getLogger("httpx").info("GET https://example.com/?token=%s", secret)
        raise RuntimeError(secret)

    repo.batch_insert_jobs.side_effect = fail
    with pytest.raises(receiver.HTTPException):
        await invoke(ingestion, header=secret + "\nAuthorization: secret")
    assert events(caplog)
    assert secret not in caplog.text
    for record in caplog.records:
        assert secret not in record.getMessage()
        assert not record.exc_info
    assert events(caplog)[-1]["actor_run_id"].startswith("sha256:")


@pytest.mark.asyncio
async def test_real_repository_retry_logs_do_not_leak_partial_errors(
    ingestion, caplog, monkeypatch
):
    from config.settings import Settings
    from repositories.raw_jobs_repo import RawJobRepository

    secret = "PRIVATE_BIGQUERY_ERROR_CANARY"
    client = MagicMock()
    client.query_and_wait.return_value = []
    client.insert_rows_json.return_value = [{"index": 0, "errors": [{"message": secret}]}]
    settings = Settings.model_construct(gcp_project_id="test-project")
    repo = RawJobRepository(bq_client=client, dataset="test", settings=settings)
    monkeypatch.setattr(receiver, "RawJobRepository", lambda: repo)
    monkeypatch.setattr("utils.retry.asyncio.sleep", AsyncMock())
    with pytest.raises(receiver.HTTPException):
        await invoke(ingestion)
    assert client.insert_rows_json.call_count == 3  # Retry policy unchanged.
    assert secret not in caplog.text
    final = events(caplog)[-1]
    assert final["persistence_outcome"] == "unconfirmed" and final["inserted"] is None
    assert any(record["event"] == "ingestion.operation.retry" for record in events(caplog))


def test_request_id_validation_and_context_reset(caplog):
    upstream = str(uuid.uuid4())
    with request_context(upstream) as first:
        emit("ingestion.webhook.received")
        assert first.parent_request_id_hash and first.request_id != upstream
    with request_context("bad\nheader") as second:
        emit("ingestion.webhook.received")
        assert second.parent_request_id_hash is None
        assert first.request_id != second.request_id
    assert context() is None
    assert upstream not in caplog.text and "bad\nheader" not in caplog.text


@pytest.mark.asyncio
async def test_concurrent_request_context_isolation():
    async def one(name):
        with request_context(None) as state:
            state.dataset_id = name
            await asyncio.sleep(0)
            assert context().dataset_id == name
            return state.request_id

    ids = await asyncio.gather(one("dataset-a"), one("dataset-b"))
    assert len(set(ids)) == 2 and context() is None


def test_queue_output_is_json_and_logging_setup_preserves_existing_handlers():
    output = io.StringIO()
    root_handlers = list(logging.getLogger().handlers)
    with patch("observability.ingestion_logging.sys.stdout", output):
        listener = configure_ingestion_logging()
        try:
            with request_context(None):
                emit("ingestion.webhook.received")
        finally:
            stop_ingestion_logging(listener)
    record = json.loads(output.getvalue())
    assert record["event"] == "ingestion.webhook.received"
    assert record["severity"] == "INFO" and record["request_id"]
    assert logging.getLogger().handlers == root_handlers


@pytest.mark.asyncio
async def test_verified_failed_actor_run_is_an_operation_error(ingestion, caplog):
    ingestion[3].return_value["status"] = "FAILED"
    with pytest.raises(receiver.HTTPException) as error:
        await invoke(ingestion)
    assert error.value.status_code == 422
    final = events(caplog)[-1]
    assert final["severity"] == "ERROR" and final["stage"] == "actor_verification"
    assert final["error_category"] == "operation_failed"


@pytest.mark.asyncio
async def test_existing_and_recovered_counts_are_distinct(ingestion, caplog):
    repo = ingestion[2]
    repo.batch_insert_jobs.side_effect = None
    repo.batch_insert_jobs.return_value = []
    repo.get_unembedded_job_ids.return_value = ["job-logging"]
    result = await invoke(ingestion)
    assert result["inserted"] == 0 and result["recovered"] == 1
    persistence = next(r for r in events(caplog) if r["event"] == "ingestion.bigquery.completed")
    assert persistence["existing_duplicates"] == 1
    assert events(caplog)[-1]["recovered"] == 1


@pytest.mark.asyncio
async def test_actual_publisher_aggregates_failures_without_secret_messages(caplog):
    publisher = MagicMock()
    publisher.publish.return_value.result.side_effect = [
        "ack",
        TimeoutError("SECRET"),
        RuntimeError("SECRET"),
    ]
    with patch.object(receiver.pubsub_v1, "PublisherClient", return_value=publisher):
        with request_context(None):
            published, failed = await receiver._publish_job_ids(
                ["one", "two", "three"], project_id="test-project", topic="test-topic"
            )
    assert (published, failed) == (1, 2)
    assert publisher.publish.call_count == 3
    failures = [r for r in events(caplog) if r["event"] == "ingestion.pubsub.publish_unconfirmed"]
    assert len(failures) == 1 and failures[0]["severity"] == "WARNING"
    assert "SECRET" not in caplog.text


def test_observability_flush_does_not_log_exception_secret(caplog):
    from observability import setup

    with patch.object(setup, "get_langfuse") as client:
        client.return_value.flush.side_effect = RuntimeError("LANGFUSE_SECRET_CANARY")
        setup.flush_observability()
    assert "LANGFUSE_SECRET_CANARY" not in caplog.text
    assert not any(r.exc_info for r in caplog.records)
    assert "RuntimeError" in caplog.text
