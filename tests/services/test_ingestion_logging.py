"""Semantic logging regressions with mocked transport/persistence and synthetic secrets."""

import asyncio
import hashlib
import hmac
import io
import json
import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from observability.ingestion_logging import (
    IngestionFormatter,
    configure_ingestion_logging,
    context,
    emit,
    request_context,
)
from services import apify_receiver as receiver
from tests.services.test_apify_receiver import _build_mock_request, _make_test_settings
from utils.retry import async_retry, with_retry


@pytest.fixture(params=["SUCCEEDED", "ABORTED"])
def ingestion(request, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="ingestion.events")
    resource = {
        "id": "run-logging",
        "actId": "actor-logging",
        "status": request.param,
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
    monkeypatch.setattr(
        receiver, "get_settings", lambda: _make_test_settings(apify_ingest_aborted_runs=True)
    )
    monkeypatch.setattr(receiver, "RawJobRepository", lambda: repo)
    monkeypatch.setattr(receiver, "_verify_actor_run", verify)
    monkeypatch.setattr(receiver, "_fetch_dataset_items", fetch)
    monkeypatch.setattr(receiver, "_publish_job_ids", publish)
    restore = configure_ingestion_logging(environment="development", level="DEBUG")
    try:
        yield resource, item, repo, verify, fetch, publish
    finally:
        restore()


def events(caplog):
    return [record.event_fields for record in caplog.records if hasattr(record, "event_fields")]


async def invoke(ingestion, header=None):
    resource = ingestion[0]
    headers = [(b"x-request-id", header.encode())] if header else None
    return await receiver.apify_webhook(
        _build_mock_request(
            {"eventType": f"ACTOR.RUN.{resource['status']}", "resource": resource},
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
    assert final["source_run_status"] == ingestion[0]["status"]
    names = [r["event"] for r in records]
    for started, completed in (
        ("ingestion.actor.verification_started", "ingestion.actor.verified"),
        ("ingestion.dataset.fetch_started", "ingestion.dataset.fetched"),
        ("ingestion.normalization.started", "ingestion.normalization.completed"),
        ("ingestion.bigquery.started", "ingestion.bigquery.completed"),
        ("ingestion.recovery.started", "ingestion.recovery.completed"),
        ("ingestion.pubsub.started", "ingestion.pubsub.completed"),
    ):
        assert names.index(started) < names.index(completed)
    assert len([name for name in names if name in TERMINAL_EVENTS]) == 1
    aborted = [record for record in records if record["event"] == "ingestion.actor.aborted"]
    assert len(aborted) == int(ingestion[0]["status"] == "ABORTED")
    if aborted:
        assert aborted[0]["severity"] == "WARNING"
        assert aborted[0]["reason"] == "partial_source_dataset"
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
        receiver,
        "get_settings",
        lambda: _make_test_settings(apify_api_token=secret, apify_ingest_aborted_runs=True),
    )

    @async_retry(max_attempts=1)
    async def fail(jobs):
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
    retries = [record.retry_fields for record in caplog.records if hasattr(record, "retry_fields")]
    assert [record["attempt"] for record in retries] == [1, 2, 3]


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


@pytest.mark.parametrize("environment", ["development", "production", "staging", ""])
def test_output_and_setup_preserve_existing_handlers(environment):
    output = io.StringIO()
    root = logging.getLogger()
    handler = logging.StreamHandler(output)
    original = logging.Formatter("original: %(message)s")
    handler.setFormatter(original)
    root.addHandler(handler)
    root_handlers = list(root.handlers)
    levels = {
        logging.getLogger(name): logging.getLogger(name).level
        for name in ("", "httpx", "httpcore", "ingestion.events")
    }
    uvicorn_handlers = list(logging.getLogger("uvicorn.error").handlers)
    try:
        restore = configure_ingestion_logging(environment=environment)
        try:
            assert configure_ingestion_logging(environment=environment) is restore
            with request_context(None):
                emit("ingestion.webhook.received")
            root.warning("unrelated")
        finally:
            restore()
            restore()
        lines = output.getvalue().splitlines()
        assert len(lines) == 2 and lines[1] == "original: unrelated"
        if environment == "development":
            assert "INFO  [Apify] Webhook received" in lines[0]
            assert "request_id=" not in lines[0] and "stage=" not in lines[0]
        else:
            record = json.loads(lines[0])
            assert record["event"] == "ingestion.webhook.received"
            assert record["severity"] == "INFO" and record["request_id"]
        assert handler.formatter is original and root.handlers == root_handlers
        assert logging.getLogger("uvicorn.error").handlers == uvicorn_handlers
        assert all(target.level == previous for target, previous in levels.items())
    finally:
        root.removeHandler(handler)
        handler.close()


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


@pytest.mark.parametrize("level,visible", [("INFO", False), ("DEBUG", True)])
def test_development_debug_is_explicit_and_transport_logs_stay_quiet(level, visible):
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        restore = configure_ingestion_logging(environment="development", level=level)
        try:
            with request_context(None):
                emit("ingestion.record.rejected", logging.DEBUG, reason="invalid_field")
            for name in ("httpx", "httpcore"):
                logging.getLogger(name).info("https://example.com/?token=TRANSPORT_SECRET")
                logging.getLogger(name).debug("TRANSPORT_SECRET")
        finally:
            restore()
        assert ("ingestion.record.rejected" in output.getvalue()) is visible
        assert "TRANSPORT_SECRET" not in output.getvalue()
    finally:
        root.removeHandler(handler)
        handler.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["hmac", "shared", "bearer", "invalid", "skipped", "missing_production"]
)
async def test_authentication_events_reflect_comparisons(ingestion, monkeypatch, caplog, mode):
    secret = "AUTHENTICATION_SECRET_CANARY"
    settings = _make_test_settings(
        apify_ingest_aborted_runs=True,
        apify_webhook_secret="" if mode in ("skipped", "missing_production") else secret,
        environment="production" if mode == "missing_production" else "development",
    )
    monkeypatch.setattr(receiver, "get_settings", lambda: settings)
    resource = ingestion[0]
    payload = {"eventType": f"ACTOR.RUN.{resource['status']}", "resource": resource}
    body = json.dumps(payload).encode()
    headers = {
        "hmac": [
            (
                b"x-apify-signature",
                hmac.new(secret.encode(), body, hashlib.sha256).hexdigest().encode(),
            )
        ],
        "shared": [(b"x-apify-webhook-secret", secret.encode())],
        "bearer": [(b"authorization", f"Bearer {secret}".encode())],
    }.get(mode)
    request = _build_mock_request(body, headers=headers)
    if mode in ("invalid", "missing_production"):
        with pytest.raises(receiver.HTTPException):
            await receiver.apify_webhook(request)
    else:
        await receiver.apify_webhook(request)
    authentication = [
        r for r in events(caplog) if r["event"].startswith("ingestion.authentication.")
    ]
    if mode in ("invalid", "missing_production"):
        assert authentication == []
    else:
        assert len(authentication) == 1
        verified = mode != "skipped"
        assert authentication[0]["verified"] is verified
        assert authentication[0]["authentication_required"] is verified
        assert authentication[0]["event"] == (
            "ingestion.authentication.verified" if verified else "ingestion.authentication.skipped"
        )
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_retry_formatter_preserves_request_correlation_without_filters():
    output = io.StringIO()
    root = logging.getLogger()
    handler = logging.StreamHandler(output)
    root.addHandler(handler)
    filters = {name: list(logging.getLogger(name).filters) for name in ("httpx", "utils.retry")}
    error = RuntimeError("RETRY_SECRET_CANARY")

    @async_retry(max_attempts=2, base_delay=0)
    async def operation():
        raise error

    try:
        restore = configure_ingestion_logging(environment="production")
        try:
            with request_context(None) as state:
                state.stage = "bigquery"
                with pytest.raises(RuntimeError) as caught:
                    await operation()
                assert caught.value is error
        finally:
            restore()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(records) == 2
        assert all(r["event"] == "ingestion.operation.retry" for r in records)
        assert all(
            r["request_id"] == state.request_id and r["stage"] == "bigquery" for r in records
        )
        assert [r["attempt"] for r in records] == [1, 2]
        assert records[-1]["outcome"] == "exhausted"
        assert "RETRY_SECRET_CANARY" not in output.getvalue()
        assert all(
            logging.getLogger(name).filters == original for name, original in filters.items()
        )
    finally:
        root.removeHandler(handler)
        handler.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("reraise", [False, True])
@pytest.mark.parametrize("succeeds", [False, True])
async def test_retry_execution_unchanged(asynchronous, reraise, succeeds, caplog):
    error = RuntimeError("RETRY_EXECUTION_SECRET")
    count = 0

    def attempt():
        nonlocal count
        count += 1
        if succeeds and count == 3:
            return "done"
        raise error

    async def async_attempt():
        return attempt()

    decorator = async_retry if asynchronous else with_retry
    operation = decorator(
        max_attempts=3, base_delay=1, backoff_factor=3, max_delay=2, reraise=reraise
    )(async_attempt if asynchronous else attempt)
    with patch("utils.retry.asyncio.sleep", new_callable=AsyncMock) as async_sleep:
        with patch("utils.retry.time.sleep") as sync_sleep:

            async def invoke_operation():
                return await operation() if asynchronous else operation()

            if reraise and not succeeds:
                with pytest.raises(RuntimeError) as caught:
                    await invoke_operation()
                assert caught.value is error
            else:
                assert await invoke_operation() == ("done" if succeeds else None)
            calls = async_sleep.await_args_list if asynchronous else sync_sleep.call_args_list
            assert [call.args[0] for call in calls] == [1, 2]
            assert count == 3
    assert "RETRY_EXECUTION_SECRET" not in caplog.text
    assert all(not record.exc_info for record in caplog.records)


@pytest.mark.parametrize(
    "value,expected", [(None, "INFO"), ("debug", "DEBUG"), ("INFO", "INFO"), ("invalid", None)]
)
def test_ingestion_log_level_settings(monkeypatch, value, expected):
    from pydantic import ValidationError

    from config.settings import Settings

    monkeypatch.delenv("INGESTION_LOG_LEVEL", raising=False)
    if value is not None:
        monkeypatch.setenv("INGESTION_LOG_LEVEL", value)
    values = dict(
        _env_file=None,
        gcp_project_id="test-project",
        litellm_proxy_base_url="http://localhost:4000",
        litellm_proxy_api_key="dummy",
        telegram_bot_token="dummy",
        e2b_api_key="dummy",
        apify_api_token="dummy",
    )
    with patch("config.settings._fetch_secret", side_effect=AssertionError("No secrets")):
        if expected is None:
            with pytest.raises(ValidationError):
                Settings(**values)
        else:
            assert Settings(**values).ingestion_log_level == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "setup", "flush"])
async def test_receiver_lifespan_restores_logging_on_exit(monkeypatch, failure):
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_formatters = {handler: handler.formatter for handler in root.handlers}
    original_levels = {
        logging.getLogger(name): logging.getLogger(name).level
        for name in ("", "ingestion.events", "httpx", "httpcore")
    }
    monkeypatch.setattr(
        receiver, "get_settings", lambda: _make_test_settings(ingestion_log_level="DEBUG")
    )
    setup, flush = MagicMock(), MagicMock()
    if failure:
        (setup if failure == "setup" else flush).side_effect = RuntimeError(
            "mock lifecycle failure"
        )
    monkeypatch.setattr(receiver, "setup_observability", setup)
    monkeypatch.setattr(receiver, "flush_observability", flush)

    async def cycle():
        async with receiver.lifespan(receiver.app):
            assert logging.getLogger("ingestion.events").level == logging.DEBUG
            assert logging.getLogger("httpx").level == logging.WARNING

    for _ in range(2):
        if failure:
            with pytest.raises(RuntimeError):
                await cycle()
        else:
            await cycle()
        assert root.handlers == original_handlers
        assert all(
            handler.formatter is formatter for handler, formatter in original_formatters.items()
        )
        assert all(target.level == level for target, level in original_levels.items())


def test_fallback_handler_is_removed_when_logging_was_unconfigured():
    root = logging.getLogger()
    output = io.StringIO()
    original_level = root.level
    with (
        patch.object(root, "handlers", []),
        patch("observability.ingestion_logging.sys.stdout", output),
    ):
        restore = configure_ingestion_logging(environment="production")
        try:
            emit("ingestion.webhook.received")
            assert len(root.handlers) == 1
        finally:
            restore()
        assert root.handlers == [] and root.level == original_level
    assert json.loads(output.getvalue())["event"] == "ingestion.webhook.received"


def compact_record(event, level=logging.INFO, **values):
    """Deterministic synthetic LogRecord; no payloads, clients or application settings."""
    fields = {
        "timestamp": "2026-11-16T06:11:43+00:00",
        "severity": logging.getLevelName(level),
        "message": event,
        "event": event,
        "request_id": "a12bc3456789abcdef",
        "actor_run_id": "run-identifier",
        "dataset_id": "dataset-identifier",
        "stage": "pubsub",
        "duration_ms": 2210,
        **values,
    }
    return logging.makeLogRecord(
        {
            "name": "ingestion.events",
            "levelno": level,
            "levelname": logging.getLevelName(level),
            "msg": json.dumps(fields),
            "event": event,
            "event_fields": fields,
        }
    )


@pytest.mark.parametrize(
    "event,fields,expected",
    [
        ("ingestion.webhook.received", {}, "[Apify] Webhook received"),
        ("ingestion.dataset.verified", {"verified": True}, "[Apify] Run and dataset verified"),
        ("ingestion.dataset.verified", {"verified": False}, "dataset verification incomplete"),
        ("ingestion.actor.aborted", {"source_run_status": "ABORTED"}, "ABORTED, partial source"),
        ("ingestion.dataset.fetched", {"received": 215}, "[Fetch] 215 records (2.21s)"),
        (
            "ingestion.normalization.completed",
            {"unique": 113, "in_batch_duplicates": 102, "rejected": 0},
            "113 unique, 102 duplicates (2.21s)",
        ),
        (
            "ingestion.normalization.completed",
            {"unique": 2, "in_batch_duplicates": 1, "rejected": 3},
            "2 unique, 1 duplicates, 3 rejected",
        ),
        (
            "ingestion.bigquery.completed",
            {"inserted": 110, "existing_duplicates": 3},
            "[BigQuery] 110 inserted, 3 existing",
        ),
        (
            "ingestion.recovery.completed",
            {"recovered": 3, "outcome": "success"},
            "[Recovery] 3 jobs recovered",
        ),
        (
            "ingestion.recovery.completed",
            {"recovered": 0, "outcome": "unconfirmed"},
            "[Recovery] Recovery unconfirmed",
        ),
        (
            "ingestion.recovery.failed",
            {"error_type": "RuntimeError"},
            "Recovery check failed: RuntimeError",
        ),
        (
            "ingestion.pubsub.completed",
            {"published": 113, "publish_failures": 0},
            "[Pub/Sub] 113 published, 0 failed",
        ),
        (
            "ingestion.pubsub.completed",
            {"published": 2, "publish_failures": 1},
            "[Pub/Sub] 2 published, 1 failed",
        ),
        ("ingestion.completed", {"outcome": "success"}, "[Complete] Ingestion successful"),
        ("ingestion.completed", {"outcome": "partial"}, "[Complete] Ingestion partially completed"),
        ("ingestion.completed", {"outcome": "empty"}, "[Complete] No records to ingest"),
        (
            "ingestion.completed",
            {"outcome": "completed_with_rejections", "rejected": 5},
            "completed with 5 rejected records",
        ),
        (
            "ingestion.completed",
            {"outcome": "publication_unconfirmed"},
            "[Complete] Publication unconfirmed",
        ),
        (
            "ingestion.failed",
            {"persistence_outcome": "unconfirmed", "inserted": None},
            "Persistence unconfirmed; stored row count unknown",
        ),
        (
            "ingestion.failed",
            {
                "stage": "authentication",
                "error_category": "request_rejected",
                "status_code": 401,
                "error_type": "HTTPException",
            },
            "[Auth] Request rejected (HTTPException, HTTP 401)",
        ),
        (
            "ingestion.operation.retry",
            {
                "stage": "bigquery",
                "attempt": 1,
                "max_attempts": 3,
                "error_type": "RuntimeError",
                "retry_delay_ms": 1500,
            },
            "[BigQuery] Retry 1/3: RuntimeError; next attempt in 1.50s",
        ),
        (
            "ingestion.operation.retry",
            {"attempt": 3, "outcome": "exhausted", "error_type": "RuntimeError"},
            "Retries exhausted after 3 attempts",
        ),
        ("ingestion.ignored", {}, "[Apify] Webhook ignored"),
        (
            "ingestion.future_event",
            {"reason": "safe_reason"},
            "ingestion.future_event — safe_reason",
        ),
    ],
)
def test_compact_summaries_preserve_structured_fields(event, fields, expected):
    record = compact_record(event, **fields)
    original = dict(record.event_fields)
    text = IngestionFormatter(None, development=True).format(record)
    assert expected in text
    assert "run-identifier" not in text and "dataset-identifier" not in text
    assert "a12bc345" not in text
    assert record.event_fields == original
    assert json.loads(IngestionFormatter(None, development=False).format(record)) == original


def test_warning_references_and_timestamp_are_compact():
    from datetime import datetime

    record = compact_record("ingestion.actor.aborted", logging.WARNING)
    text = IngestionFormatter(None, development=True).format(record)
    local_time = (
        datetime.fromisoformat(record.event_fields["timestamp"]).astimezone().strftime("%H:%M:%S")
    )
    assert text.startswith(local_time + " WARN ")
    assert "[a12bc345]" in text and record.event_fields["request_id"] not in text


@pytest.mark.parametrize(
    "published,observed,worker_line",
    [(113, "not_observed", True), (0, "not_observed", False), (113, None, False)],
)
def test_worker_note_is_only_a_view_of_confirmed_publication(published, observed, worker_line):
    record = compact_record(
        "ingestion.completed",
        outcome="success",
        published=published,
        subscriber_processing=observed,
        duration_ms=32141,
    )
    text = IngestionFormatter(None, development=True).format(record)
    assert "Ingestion successful (32.14s)" in text
    assert ("[Worker] Processing not verified" in text) is worker_line
    assert len(text.splitlines()) == (2 if worker_line else 1)
    assert record.event_fields["event"] == "ingestion.completed"


@pytest.mark.parametrize("level,verbose", [("INFO", False), ("DEBUG", True)])
def test_console_compaction_does_not_suppress_full_file_capture(level, verbose):
    output = io.StringIO()
    console = logging.StreamHandler(output)
    # Exercise actual FileHandler setup with an in-memory stream; no filesystem I/O.
    file_output = io.StringIO()
    file_handler = logging.FileHandler("unused-ingestion-test.jsonl", encoding="utf-8", delay=True)
    file_handler.stream = file_output
    root = logging.getLogger()
    root.addHandler(console)
    root.addHandler(file_handler)
    original_filters = list(console.filters)
    try:
        restore = configure_ingestion_logging(environment="development", level=level)
        try:
            with request_context(None) as state:
                state.source_run_status = "ABORTED"
                state.actor_run_id, state.dataset_id = "run-full", "dataset-full"
                emit("ingestion.webhook.received")
                emit("ingestion.authentication.verified", verified=True)
                emit("ingestion.dataset.verified", verified=True)
                emit("ingestion.actor.aborted", logging.WARNING, reason="partial_source_dataset")
                emit("ingestion.future_event", reason="safe_reason")
                emit("ingestion.normalization.started")
                emit("ingestion.normalization.started", logging.WARNING, reason="unexpected")
        finally:
            restore()
        file_handler.flush()
        captured = [json.loads(line) for line in file_output.getvalue().splitlines()]
        assert len(captured) == 7
        assert all(
            r["request_id"] == state.request_id and r["dataset_id"] == "dataset-full"
            for r in captured
        )
        lines = output.getvalue().splitlines()
        headers = [line for line in lines if line and not line.startswith("    ")]
        assert len(headers) == (7 if verbose else 5)
        assert ("\n\n" in output.getvalue()) is verbose
        assert ("ingestion.authentication.verified" in output.getvalue()) is verbose
        assert (state.request_id in output.getvalue()) is verbose
        assert "safe_reason" in output.getvalue() and "unexpected" in output.getvalue()
        assert console.filters == original_filters
    finally:
        root.removeHandler(console)
        root.removeHandler(file_handler)
        console.close()
        file_handler.close()


def test_verbose_console_separates_headers_and_wraps_metadata():
    record = compact_record("ingestion.dataset.fetched", received=215)
    original = dict(record.event_fields)
    text = IngestionFormatter(None, development=True, verbose=True).format(record)
    lines = text.splitlines()
    assert "INFO  ingestion.dataset.fetched" in lines[0]
    assert "request_id=" not in lines[0]
    assert all(line.startswith("    ") for line in lines[1:])
    assert all(len(line) <= 100 for line in lines)
    assert 'request_id="a12bc3456789abcdef"' in text
    assert 'actor_run_id="run-identifier"' in text
    assert 'dataset_id="dataset-identifier"' in text
    assert "received=215" in text and text.endswith("\n")
    assert record.event_fields == original
    production = IngestionFormatter(None, development=False, verbose=True).format(record)
    assert "\n" not in production and json.loads(production) == original


@pytest.mark.asyncio
async def test_startup_reports_effective_bigquery_location_without_a_webhook(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="ingestion.events")
    configured = _make_test_settings()
    configured.bq_location = "US"
    configured.publication_ledger_enabled = False
    monkeypatch.setattr(receiver, "get_settings", lambda: configured)
    monkeypatch.setattr(receiver, "setup_observability", MagicMock())
    monkeypatch.setattr(receiver, "flush_observability", MagicMock())
    with patch.object(receiver, "RawJobRepository") as repository:
        async with receiver.lifespan(receiver.app):
            pass
    repository.assert_not_called()
    runtime = next(r for r in events(caplog) if r["event"] == "ingestion.runtime.configured")
    assert runtime["bq_location"] == "US" and runtime["ledger_enabled"] is False
    assert "BigQuery location=US; ledger=disabled" in IngestionFormatter(
        None, development=True,
    ).format(next(r for r in caplog.records if getattr(r, "event", None) == "ingestion.runtime.configured"))


def test_runtime_config_does_not_render_arbitrary_location_contents(caplog):
    caplog.set_level(logging.INFO, logger="ingestion.events")
    emit("ingestion.runtime.configured", bq_location="SECRET_LOCATION_CANARY", ledger_enabled=False)
    runtime = next(r for r in events(caplog) if r["event"] == "ingestion.runtime.configured")
    assert runtime["bq_location"] == "unrecognized"
    assert "SECRET_LOCATION_CANARY" not in caplog.text


TERMINAL_EVENTS = {
    "ingestion.completed",
    "ingestion.failed",
    "ingestion.ignored",
    "ingestion.cancelled",
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("event", 42),
        ("event", True),
        ("event", ["unsupported"]),
        ("event", {"unexpected": "METADATA_SECRET_CANARY"}),
        ("event", "ACTOR.RUN.FAILED"),
        ("status", 42),
        ("status", True),
        ("status", False),
        ("status", 0),
        ("status", None),
        ("status", ["unsupported"]),
        ("status", {"unexpected": "METADATA_SECRET_CANARY"}),
        ("status", "RUNNING"),
    ],
)
async def test_ignored_metadata_has_one_terminal_event_without_changing_response(
    ingestion, monkeypatch, caplog, field, value
):
    resource, _, repo, verify, fetch, publish = ingestion
    payload = {"eventType": f"ACTOR.RUN.{resource['status']}", "resource": dict(resource)}
    if field == "event":
        payload["eventType"] = value
        message = (
            f"Event type '{value}' ignored; only ACTOR.RUN.SUCCEEDED "
            "and enabled ACTOR.RUN.ABORTED events are processed."
        )
    else:
        payload["resource"]["status"] = value
        message = f"Actor run status '{value}' is not {resource['status']}; skipped."
    monkeypatch.setattr(receiver, "perf_counter", lambda: context().started + 2.5)
    response = await receiver.apify_webhook(_build_mock_request(payload))
    assert response == {"received": value, "inserted": 0, "published": 0, "message": message}
    terminal = [r for r in events(caplog) if r["event"] in TERMINAL_EVENTS]
    assert len(terminal) == 1 and terminal[0]["event"] == "ingestion.ignored"
    assert terminal[0]["outcome"] == "ignored" and terminal[0]["duration_ms"] == 2500
    assert terminal[0]["received"] is None  # Event/status values are not record counts.
    verify.assert_not_awaited()
    fetch.assert_not_awaited()
    repo.batch_insert_jobs.assert_not_awaited()
    publish.assert_not_awaited()
    assert context() is None and "METADATA_SECRET_CANARY" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_falsy_event_type_preserves_existing_rejection(ingestion, caplog, value):
    with pytest.raises(receiver.HTTPException) as error:
        await receiver.apify_webhook(_build_mock_request({"eventType": value}))
    assert error.value.status_code == 422
    terminal = [r for r in events(caplog) if r["event"] in TERMINAL_EVENTS]
    assert len(terminal) == 1 and terminal[0]["event"] == "ingestion.failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,stage",
    [
        ("verify", "actor_verification"),
        ("fetch", "dataset_fetch"),
        ("insert", "bigquery"),
        ("recover", "recovery"),
        ("publish", "pubsub"),
    ],
)
async def test_cancellation_logs_once_and_propagates(ingestion, caplog, operation, stage):
    _, _, repo, verify, fetch, publish = ingestion
    cancelled = asyncio.CancelledError("CANCELLATION_SECRET_CANARY")
    if operation == "recover":
        repo.batch_insert_jobs.side_effect = None
        repo.batch_insert_jobs.return_value = []
    target = {
        "verify": verify,
        "fetch": fetch,
        "insert": repo.batch_insert_jobs,
        "recover": repo.get_unembedded_job_ids,
        "publish": publish,
    }[operation]
    target.side_effect = cancelled
    with pytest.raises(asyncio.CancelledError) as caught:
        await invoke(ingestion)
    assert caught.value is cancelled
    terminal = [r for r in events(caplog) if r["event"] in TERMINAL_EVENTS]
    assert len(terminal) == 1
    final = terminal[0]
    assert final["event"] == "ingestion.cancelled" and final["severity"] == "WARNING"
    assert final["outcome"] == "cancelled" and final["stage"] == stage
    assert final["duration_ms"] >= 0 and final["error_type"] == "CancelledError"
    assert final["published"] is None and final["publish_failures"] is None
    assert "status_code" not in final
    if operation in ("verify", "fetch", "insert"):
        assert final["inserted"] is None
    if operation == "verify":
        fetch.assert_not_awaited()
    if operation in ("verify", "fetch"):
        repo.batch_insert_jobs.assert_not_awaited()
    if operation == "insert":
        assert final["persistence_outcome"] == "unconfirmed"
        repo.get_unembedded_job_ids.assert_not_awaited()
    if operation == "publish":
        assert final["inserted"] == 1
        assert final["reason"] == "publication_confirmation_incomplete"
    else:
        publish.assert_not_awaited()
    assert context() is None and "CANCELLATION_SECRET_CANARY" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("items", [[], [None, {}]])
async def test_early_exit_has_no_fabricated_stage_execution(ingestion, caplog, items):
    ingestion[4].return_value = items
    await invoke(ingestion)
    names = [r["event"] for r in events(caplog)]
    assert ("ingestion.normalization.started" in names) is bool(items)
    for name in ("bigquery", "recovery", "pubsub"):
        assert f"ingestion.{name}.started" not in names
    assert [name for name in names if name in TERMINAL_EVENTS] == ["ingestion.completed"]


def test_compact_info_shows_long_stage_starts_and_cancellation():
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        restore = configure_ingestion_logging(environment="development", level="INFO")
        try:
            with request_context(None) as state:
                for event, fields in (
                    ("ingestion.actor.verification_started", {}),
                    ("ingestion.dataset.fetch_started", {}),
                    ("ingestion.normalization.started", {"received": 215}),
                    ("ingestion.bigquery.started", {"unique": 113}),
                    ("ingestion.recovery.started", {}),
                    ("ingestion.pubsub.started", {}),
                ):
                    emit(event, **fields)
                state.stage = "bigquery"
                emit(
                    "ingestion.cancelled",
                    logging.WARNING,
                    outcome="cancelled",
                    persistence_outcome="unconfirmed",
                )
        finally:
            restore()
        text = output.getvalue()
        expected = [
            "[Apify] Verifying run...",
            "[Fetch] Fetching dataset...",
            "[Normalize] Normalizing and deduplicating 215 records...",
            "[BigQuery] Checking and saving 113 unique jobs...",
            "[Recovery] Checking recovery...",
            "[Pub/Sub] Publishing jobs...",
            "[BigQuery] Ingestion cancelled; persistence unconfirmed, stored row count unknown",
        ]
        headers = [line for line in text.splitlines() if not line.startswith("    ")]
        assert len(headers) == 7
        assert sum(" INFO " in line for line in headers) == 6
        assert "tests/services/test_ingestion_logging.py:" in text
        assert [text.index(part) for part in expected] == sorted(
            text.index(part) for part in expected
        )
        assert f"[{state.request_id[:8]}]" in text
        assert "[Worker]" not in text
    finally:
        root.removeHandler(handler)
        handler.close()
