"""Source attribution and privacy regressions; SDK calls are always mocked."""

import io
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import BadRequest, ServiceUnavailable

from observability import diagnostics
from observability.diagnostics import diagnostic_operation, exception_fields
from observability.ingestion_logging import (
    IngestionFormatter,
    configure_ingestion_logging,
    emit,
    error_fields,
    request_context,
)
from repositories.raw_jobs_repo import RawJobRepository
from tests.services.test_embedding_worker import _make_dummy_job
from config.settings import Settings


def sdk_raise(error):
    raise error


# A real traceback whose deepest frame resembles an installed SDK, including a
# venv physically INSIDE the repository. No generated source or external calls.
sdk_raise.__code__ = sdk_raise.__code__.replace(
    co_filename=str(
        diagnostics._ROOT / "venv/Lib/site-packages/google/cloud/_http/__init__.py",
    )
)


def application_failure(error, operation="duplicate_check"):
    with diagnostic_operation("bigquery", operation):
        sdk_raise(error)


def sdk_error(reason="invalidQuery", error_type=BadRequest):
    error = error_type(
        "RAW_EXCEPTION_SECRET",
        errors=[
            {
                "reason": reason,
                "message": "SQL_AND_TOKEN_SECRET",
                "location": "PAYLOAD_SECRET",
            }
        ],
    )
    error.job_id = "harvester_safe_query_id"
    return error


def fields_for(error, operation="duplicate_check"):
    try:
        application_failure(error, operation)
    except Exception as caught:
        return exception_fields(caught)
    raise AssertionError("Expected an exception")


def test_emitter_is_automatic_relative_and_record_source_is_correct(caplog):
    caplog.set_level(logging.INFO, logger="ingestion.events")
    with request_context(None) as state:
        emit("ingestion.webhook.received")
    record = next(record for record in caplog.records if hasattr(record, "event_fields"))
    source = record.event_fields["source"]
    assert source["file"] == "tests/services/test_source_diagnostics.py"
    assert source["function"] == "test_emitter_is_automatic_relative_and_record_source_is_correct"
    assert source["line"] == record.lineno and record.funcName == source["function"]
    assert record.event_fields["request_id"] == state.request_id
    assert "Webhook received" in IngestionFormatter(None, development=True).format(record)
    assert "test_source_diagnostics.py" not in IngestionFormatter(None, development=True).format(
        record
    )


def test_failure_prefers_last_application_frame_not_sdk_or_wrapper():
    try:
        try:
            application_failure(sdk_error())
        except BadRequest as error:
            raise RuntimeError("OUTER_SECRET") from error
    except RuntimeError as wrapped:
        fields = error_fields(wrapped)
    assert fields["error_type"] == "BadRequest"
    assert fields["failure_site"]["file"] == "tests/services/test_source_diagnostics.py"
    assert fields["failure_site"]["function"] == "application_failure"
    assert "site-packages" not in fields["error_location"]
    assert fields["dependency_operation"] == "duplicate_check"
    assert fields["dependency_status"] == 400
    assert fields["dependency_reason"] == "invalidQuery"
    assert fields["query_job_id"] == "harvester_safe_query_id"


@pytest.mark.parametrize(
    "operation,reason,error_type,expected",
    [
        ("duplicate_check", "invalidQuery", BadRequest, "permanent"),
        ("duplicate_check", "invalid", BadRequest, "permanent"),
        ("duplicate_check", "jobBackendError", BadRequest, "retryable"),
        ("duplicate_check", "backendError", ServiceUnavailable, "retryable"),
        ("streaming_insert", "backendError", ServiceUnavailable, "uncertain"),
        ("streaming_insert", "invalid", BadRequest, "permanent"),
        ("duplicate_check", "UNKNOWN_REASON_SECRET", BadRequest, "unknown"),
    ],
)
def test_classification_depends_on_operation_and_structured_reason(
    operation,
    reason,
    error_type,
    expected,
):
    fields = fields_for(sdk_error(reason, error_type), operation)
    assert fields["retry_classification"] == expected
    assert "UNKNOWN_REASON_SECRET" not in json.dumps(fields)


def test_bare_bad_request_cannot_be_classified_from_its_message():
    fields = fields_for(BadRequest("invalidQuery SECRET_PAYLOAD"))
    assert fields["retry_classification"] == "unknown"
    assert "dependency_reason" not in fields
    assert "SECRET_PAYLOAD" not in json.dumps(fields)


def test_no_traceback_or_cyclic_exception_chain_is_safe():
    error = RuntimeError("SECRET")
    error.__cause__ = error
    fields = exception_fields(error)
    assert fields["error_type"] == "RuntimeError" and fields["error_location"] is None
    assert "SECRET" not in json.dumps(fields)


def test_diagnostic_metadata_properties_cannot_replace_original_exception():
    class UnusualSdkError(Exception):
        @property
        def errors(self):
            raise ValueError("METADATA_SECRET")

        @property
        def code(self):
            raise ValueError("METADATA_SECRET")

    error = UnusualSdkError("ORIGINAL_SECRET")
    fields = fields_for(error)
    assert fields["error_type"] == "UnusualSdkError"
    assert fields["retry_classification"] == "unknown"
    assert "SECRET" not in json.dumps(fields)


def test_failed_source_is_sanitized_against_known_request_secrets(caplog):
    caplog.set_level(logging.INFO, logger="ingestion.events")
    with request_context(None) as state:
        state.secrets = ["SOURCE_SECRET"]
        failure = {"file": "services/SOURCE_SECRET.py", "function": "fail", "line": 1}
        emit(
            "ingestion.failed",
            logging.ERROR,
            failure_site=failure,
            error_location=diagnostics.location_text(failure),
        )
    record = next(r for r in caplog.records if hasattr(r, "event_fields"))
    assert "SOURCE_SECRET" not in json.dumps(record.event_fields)
    assert "failure_site" not in record.event_fields


@pytest.mark.parametrize("verbose", [False, True])
def test_console_json_and_file_capture_keep_safe_failure_details_and_restore(verbose):
    root = logging.getLogger()
    console_stream, file_stream = io.StringIO(), io.StringIO()
    console = logging.StreamHandler(console_stream)
    file_handler = logging.FileHandler("unused-source-test.jsonl", delay=True)
    file_handler.stream = file_stream
    original = logging.Formatter("original %(message)s")
    console.setFormatter(original)
    file_handler.setFormatter(original)
    sentinel = logging.Filter()
    console.addFilter(sentinel)
    root.addHandler(console)
    root.addHandler(file_handler)
    try:
        restore = configure_ingestion_logging(
            environment="development", level="DEBUG" if verbose else "INFO"
        )
        try:
            with request_context(None) as state:
                state.secrets = ["RAW_EXCEPTION_SECRET", "SQL_AND_TOKEN_SECRET", "PAYLOAD_SECRET"]
                emit("ingestion.failed", logging.ERROR, **fields_for(sdk_error()))
        finally:
            restore()
            restore()
        file_handler.flush()
        structured = json.loads(file_stream.getvalue())
        assert structured["request_id"] == state.request_id
        assert structured["source"]["function"].endswith(
            "test_console_json_and_file_capture_keep_safe_failure_details_and_restore"
        )
        assert structured["failure_site"]["function"] == "application_failure"
        assert structured["dependency_reason"] == "invalidQuery"
        assert "application_failure" in console_stream.getvalue()
        assert "duplicate_check" in console_stream.getvalue()
        assert console.formatter is file_handler.formatter is original
        assert console.filters == [sentinel]
        for secret in state.secrets:
            assert secret not in console_stream.getvalue() + file_stream.getvalue()
    finally:
        root.removeHandler(console)
        root.removeHandler(file_handler)
        console.close()
        file_handler.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["duplicate_check", "streaming_insert"])
async def test_repository_boundary_survives_retries_and_outer_http_wrapper(operation, caplog):
    caplog.set_level(logging.DEBUG)
    configured = Settings.model_construct(
        gcp_project_id="test-project",
        bq_dataset="test_dataset",
        bq_location="US",
        publication_ledger_enabled=False,
    )
    client = MagicMock()
    error = sdk_error()
    client.query_and_wait.return_value = []
    client.insert_rows_json.return_value = []
    if operation == "duplicate_check":
        client.query_and_wait.side_effect = error
    else:
        client.insert_rows_json.side_effect = error
    repo = RawJobRepository(client, settings=configured)
    with patch("utils.retry.asyncio.sleep", new_callable=AsyncMock):
        with request_context(None):
            with pytest.raises(BadRequest) as caught:
                await repo.batch_insert_jobs([_make_dummy_job("job")])
            emit("ingestion.failed", logging.ERROR, **error_fields(caught.value))
    retries = [r.retry_fields for r in caplog.records if hasattr(r, "retry_fields")]
    assert len(retries) == 3  # Classification does not change the existing policy.
    assert all(r["dependency_operation"] == operation for r in retries)
    assert all(r["failure_site"]["file"] == "repositories/raw_jobs_repo.py" for r in retries)
    assert all(
        r["failure_site"]["function"] == "RawJobRepository._batch_insert_jobs" for r in retries
    )
    assert all(r["retry_classification"] == "permanent" for r in retries)
    expected_line = retries[0]["failure_site"]["line"]
    assert all(r["failure_site"]["line"] == expected_line for r in retries)
    if operation == "duplicate_check":
        client.insert_rows_json.assert_not_called()
    else:
        assert client.insert_rows_json.call_count == 3
    assert "RAW_EXCEPTION_SECRET" not in caplog.text


def test_untrusted_source_fields_cannot_override_emitter_or_leak_payload(caplog):
    caplog.set_level(logging.INFO, logger="ingestion.events")
    emit(
        "ingestion.failed",
        logging.ERROR,
        source={"file": "SECRET", "function": "SECRET", "line": 1},
        failure_site={"file": "/absolute/SECRET", "function": "SECRET", "line": 1},
        exception_message="SECRET",
        sql="SECRET",
        payload="SECRET",
    )
    fields = next(r.event_fields for r in caplog.records if hasattr(r, "event_fields"))
    assert fields["source"]["file"] == "tests/services/test_source_diagnostics.py"
    assert "failure_site" not in fields
    assert "SECRET" not in json.dumps(fields)
