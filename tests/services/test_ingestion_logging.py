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
            assert "INFO ingestion.webhook.received request_id=" in lines[0]
            assert "stage=" in lines[0] and "duration_ms=" in lines[0]
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
