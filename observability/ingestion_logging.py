"""Safe ingestion events using existing stdlib handlers and request context."""

import hashlib
import json
import logging
import re
import sys
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

COUNTERS = (
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
)
_context: ContextVar["IngestionContext | None"] = ContextVar("ingestion_log_context", default=None)
_logger = logging.getLogger("ingestion.events")
_restore_logging: Callable[[], None] | None = None


@dataclass
class IngestionContext:
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    parent_request_id_hash: str | None = None
    stage: str = "configuration"
    started: float = field(default_factory=time.perf_counter)
    stage_started: float = field(default_factory=time.perf_counter)
    actor_run_id: str | None = None
    dataset_id: str | None = None
    source_run_status: str | None = None
    secrets: list[str] = field(default_factory=list, repr=False)
    incomplete: bool = False
    record_index: int | None = None
    counters: dict[str, int | None] = field(default_factory=lambda: dict.fromkeys(COUNTERS))


def context() -> IngestionContext | None:
    return _context.get()


@contextmanager
def request_context(external_id: str | None):
    state = IngestionContext()
    # Never echo an arbitrary header. A valid UUID can still contain sensitive data;
    # retain only a one-way correlation hash, alongside a fresh server request ID.
    if external_id and len(external_id) == 36:
        try:
            parsed = uuid.UUID(external_id)
            if parsed.version == 4 and str(parsed) == external_id.lower():
                state.parent_request_id_hash = hashlib.sha256(external_id.encode()).hexdigest()[:16]
        except ValueError:
            pass
    token = _context.set(state)
    try:
        yield state
    finally:
        _context.reset(token)


def identifier(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    state = context()
    if re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", value) and not any(
        secret in value for secret in (state.secrets if state else [])
    ):
        return value
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()[:16]


def stage(name: str) -> None:
    state = context()
    if state:
        state.stage = name
        state.stage_started = time.perf_counter()


def emit(event: str, level: int = logging.INFO, **fields: object) -> None:
    record = _event_fields(event, level, fields)
    _logger.log(
        level,
        json.dumps(record, separators=(",", ":")),
        extra={"event": event, "event_fields": record},
    )


def _event_fields(event: str, level: int, fields: dict[str, object]) -> dict[str, object]:
    state = context()
    record: dict[str, object] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": logging.getLevelName(level),
        "message": event,
        "event": event,
        "request_id": state.request_id if state else None,
        "actor_run_id": identifier(state.actor_run_id) if state else None,
        "dataset_id": identifier(state.dataset_id) if state else None,
        "stage": state.stage if state else "standalone",
        "duration_ms": round((time.perf_counter() - state.stage_started) * 1000) if state else 0,
    }
    if state and state.parent_request_id_hash:
        record["parent_request_id_hash"] = state.parent_request_id_hash
    if state and state.record_index is not None:
        record["record_index"] = state.record_index
    if state and state.source_run_status in ("SUCCEEDED", "ABORTED"):
        record["source_run_status"] = state.source_run_status
    for key, value in fields.items():
        if key in COUNTERS and (value is None or type(value) is int):
            record[key] = value
            if state:
                state.counters[key] = value
        elif (
            key
            in {
                "duration_ms",
                "status_code",
                "attempt",
                "max_attempts",
                "existing_duplicates",
                "retry_delay_ms",
            }
            and type(value) is int
        ):
            record[key] = value
        elif (
            key in {"verified", "allowlist_enforced", "authentication_required"}
            and type(value) is bool
        ):
            record[key] = value
        elif key in {
            "stage",
            "outcome",
            "error_category",
            "error_type",
            "error_location",
            "operation",
            "reason",
            "persistence_outcome",
            "subscriber_processing",
        }:
            record[key] = identifier(value)
    return record


def error_fields(error: BaseException) -> dict[str, str | None]:
    cause = error.__cause__ or error
    frame = cause.__traceback__
    location = None
    if frame:
        while frame.tb_next:
            frame = frame.tb_next
        code = frame.tb_frame.f_code
        location = identifier(f"{Path(code.co_filename).name}:{frame.tb_lineno}:{code.co_name}")
    return {"error_type": identifier(type(cause).__name__), "error_location": location}


class IngestionFormatter(logging.Formatter):
    """Render safe event fields; delegate unrelated records to their original formatter."""

    def __init__(self, fallback: logging.Formatter | None, *, development: bool):
        super().__init__()
        self.fallback = fallback or logging.Formatter()
        self.development = development

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "event_fields", None)
        if record.name == "utils.retry" and context() is not None:
            retry = getattr(record, "retry_fields", None)
            if isinstance(retry, dict):
                fields = _event_fields("ingestion.operation.retry", record.levelno, retry)
        if not isinstance(fields, dict):
            return self.fallback.format(record)
        if not self.development:
            return json.dumps(fields, separators=(",", ":"))
        details = " ".join(
            f"{key}={json.dumps(value, ensure_ascii=True, separators=(',', ':'))}"
            for key, value in fields.items()
            if key not in {"timestamp", "severity", "message", "event"} and value is not None
        )
        return f"{fields['timestamp']} {fields['severity']} {fields['event']} {details}"


def configure_ingestion_logging(*, environment: str, level: str = "INFO") -> Callable[[], None]:
    """Wrap existing stream output once; return an idempotent restoration callback.

    INGESTION_LOG_LEVEL=DEBUG enables record diagnostics without enabling HTTP wire logs.
    Only explicit development gets readable output; all other environments get JSON.
    """
    global _restore_logging
    if _restore_logging is not None:
        return _restore_logging
    root = logging.getLogger()
    original_levels = {_logger: _logger.level}
    owned = None
    if not root.handlers:
        owned = logging.StreamHandler(sys.stdout)
        owned.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
            )
        )
        root.addHandler(owned)
        original_levels[root] = root.level
        root.setLevel(logging.INFO)
    original_formatters = {
        handler: handler.formatter
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
    }
    for handler, formatter in original_formatters.items():
        handler.setFormatter(
            IngestionFormatter(formatter, development=environment.lower() == "development")
        )
    _logger.setLevel(level)
    for name in ("httpx", "httpcore"):
        target = logging.getLogger(name)
        original_levels[target] = target.level
        target.setLevel(logging.WARNING)

    def restore() -> None:
        global _restore_logging
        if _restore_logging is not restore:
            return
        for handler, formatter in original_formatters.items():
            handler.setFormatter(formatter)
        for target, previous in original_levels.items():
            target.setLevel(previous)
        if owned is not None:
            root.removeHandler(owned)
            owned.close()
        _restore_logging = None

    _restore_logging = restore
    return restore
