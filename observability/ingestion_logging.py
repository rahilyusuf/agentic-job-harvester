"""Safe ingestion events using stdlib logging, request context and queued stdout."""

import hashlib
import json
import logging
import logging.handlers
import queue
import re
import sys
import time
import uuid
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
_logger.setLevel(logging.INFO)


@dataclass
class IngestionContext:
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    parent_request_id_hash: str | None = None
    stage: str = "configuration"
    started: float = field(default_factory=time.perf_counter)
    stage_started: float = field(default_factory=time.perf_counter)
    actor_run_id: str | None = None
    dataset_id: str | None = None
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
    for key, value in fields.items():
        if key in COUNTERS and (value is None or type(value) is int):
            record[key] = value
            if state:
                state.counters[key] = value
        elif (
            key in {"duration_ms", "status_code", "attempt", "max_attempts", "existing_duplicates"}
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
    _logger.log(
        level,
        json.dumps(record, separators=(",", ":")),
        extra={"event": event, "event_fields": record},
    )


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


class _RetryFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if context() is None:
            return True
        # Existing generic retry messages interpolate exception strings. Replace
        # them only within ingestion requests; do not change retry execution.
        args = record.args if isinstance(record.args, tuple) else ()
        fields: dict[str, object] = {"error_category": "operation_retry"}
        if len(args) >= 5:
            fields.update(attempt=args[1], max_attempts=args[2])
        emit("ingestion.operation.retry", record.levelno, **fields)
        return False


class _HTTPFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # HTTPX INFO includes complete URLs. Explicit stage events replace that
        # redundant automatic request log during ingestion.
        return context() is None


for _name, _filter in (("utils.retry", _RetryFilter), ("httpx", _HTTPFilter)):
    _target = logging.getLogger(_name)
    if not any(isinstance(existing, _filter) for existing in _target.filters):
        _target.addFilter(_filter())


def configure_ingestion_logging() -> logging.handlers.QueueListener:
    """Configure only ingestion output; leave application/Uvicorn handlers intact."""
    events: queue.SimpleQueue = queue.SimpleQueue()
    handler = logging.handlers.QueueHandler(events)
    output = logging.StreamHandler(sys.stdout)
    output.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    _logger.propagate = False
    listener = logging.handlers.QueueListener(events, output)
    listener.start()
    return listener


def stop_ingestion_logging(listener: logging.handlers.QueueListener) -> None:
    listener.stop()
    for handler in list(_logger.handlers):
        if isinstance(handler, logging.handlers.QueueHandler) and handler.queue is listener.queue:
            _logger.removeHandler(handler)
            handler.close()
    _logger.propagate = True
