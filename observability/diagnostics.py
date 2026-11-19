"""Local source and allowlisted exception metadata. No SDKs, settings or network I/O."""

from __future__ import annotations

import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_INFRASTRUCTURE = {
    "observability/diagnostics.py",
    "observability/ingestion_logging.py",
    "utils/retry.py",
}
_PERMANENT_REASONS = (
    "invalidQuery",
    "invalid",
    "accessDenied",
    "notFound",
    "billingNotEnabled",
    "invalidUser",
)
_TRANSIENT_REASONS = (
    "backendError",
    "internalError",
    "jobBackendError",
    "jobInternalError",
    "rateLimitExceeded",
    "jobRateLimitExceeded",
    "tableUnavailable",
)
_KNOWN_REASONS = set(
    _PERMANENT_REASONS
    + _TRANSIENT_REASONS
    + (
        "badRequest",
        "stopped",
        "duplicate",
        "quotaExceeded",
        "resourcesExceeded",
        "responseTooLarge",
    )
)


def safe_location(value: object) -> dict | None:
    if not isinstance(value, dict) or set(value) != {"file", "function", "line"}:
        return None
    filename, function, line = value["file"], value["function"], value["line"]
    if (
        not isinstance(filename, str)
        or len(filename) > 240
        or not re.fullmatch(r"(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.py", filename)
        or not isinstance(function, str)
        or len(function) > 240
        or not re.fullmatch(r"[A-Za-z0-9_.<>]+", function)
        or type(line) is not int
        or not 1 <= line <= 1000000
    ):
        return None
    return {"file": filename, "function": function, "line": line}


def _frame_source(frame: FrameType, line: int) -> dict | None:
    try:
        relative = Path(frame.f_code.co_filename).resolve().relative_to(_ROOT)
    except (ValueError, OSError, RuntimeError):
        return None
    if relative.parts[0].lower() in {"venv", ".venv", "env"} or any(
        part.lower() in {"site-packages", "dist-packages"} for part in relative.parts
    ):
        return None
    return safe_location(
        {"file": relative.as_posix(), "function": frame.f_code.co_qualname, "line": line}
    )


def caller_source(stacklevel: int = 1) -> dict | None:
    try:
        frame = sys._getframe(stacklevel)
        return _frame_source(frame, frame.f_lineno)
    except ValueError:
        return None


def location_text(value: object) -> str | None:
    location = safe_location(value)
    if location:
        return f"{location['file']}:{location['line']}:{location['function']}"
    return None


@dataclass(frozen=True)
class _Operation:
    dependency: str
    operation: str


@contextmanager
def diagnostic_operation(dependency: str, operation: str):
    """Annotate a dependency boundary without logging, wrapping or changing errors."""
    try:
        yield
    except BaseException as error:
        try:
            if not isinstance(getattr(error, "_diagnostic_operation", None), _Operation):
                # Third-party exceptions expose diagnostic metadata dynamically.
                setattr(  # noqa: B010
                    error, "_diagnostic_operation", _Operation(dependency, operation)
                )
        except Exception:
            pass  # Diagnostics must never replace the original failure/cancellation.
        raise


def _chain(error: BaseException | None) -> list[BaseException]:
    errors, seen = [], set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        errors.append(error)
        error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    return errors


def _attribute(error: BaseException, name: str):
    try:
        return getattr(error, name, None)
    except Exception:
        return None


def exception_fields(error: BaseException) -> dict[str, Any]:
    """Never formats an exception, traceback, response body, SQL or frame locals."""
    errors = _chain(error)
    fields = {"error_type": type(errors[-1]).__name__, "error_location": None}
    for cause in reversed(errors):
        frame, nearest = cause.__traceback__, None
        while frame:
            source = _frame_source(frame.tb_frame, frame.tb_lineno)
            if source and source["file"] not in _INFRASTRUCTURE:
                nearest = source
            frame = frame.tb_next
        if nearest:
            fields.update(failure_site=nearest, error_location=location_text(nearest))
            break
    operation = next(
        (
            value
            for cause in reversed(errors)
            if isinstance(value := _attribute(cause, "_diagnostic_operation"), _Operation)
        ),
        None,
    )
    if operation:
        fields.update(dependency=operation.dependency, dependency_operation=operation.operation)

    # Read only explicit SDK metadata, never parse a message or HTTP response body.
    status, reason, job_id = None, None, None
    reasons = []
    for cause in reversed(errors):
        code = _attribute(cause, "code")
        if (
            status is None
            and isinstance(code, int)
            and not isinstance(code, bool)
            and 100 <= code <= 599
        ):
            status = int(code)
        details = _attribute(cause, "errors")
        if isinstance(details, (list, tuple)):
            reasons.extend(
                item.get("reason")
                for item in details[:20]
                if isinstance(item, dict) and isinstance(item.get("reason"), str)
            )
        candidate = _attribute(cause, "job_id")
        if (
            job_id is None
            and isinstance(candidate, str)
            and re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", candidate)
        ):
            job_id = candidate
    if status is not None:
        fields["dependency_status"] = status
    reason = next((r for r in _PERMANENT_REASONS + _TRANSIENT_REASONS if r in reasons), None)
    if reason is None and reasons:
        reason = next((r for r in reasons if r in _KNOWN_REASONS), "unclassified")
    if reason:
        fields["dependency_reason"] = reason
    if operation and operation.dependency == "bigquery":
        if reason in _PERMANENT_REASONS:
            classification = "permanent"
        elif operation.operation == "streaming_insert":
            classification = "uncertain"
        elif operation.operation == "duplicate_check" and (
            reason in _TRANSIENT_REASONS or status in {429, 500, 502, 503, 504}
        ):
            classification = "retryable"
        else:
            classification = "unknown"
        fields["retry_classification"] = classification
    if job_id:
        fields["query_job_id"] = job_id
    return fields
