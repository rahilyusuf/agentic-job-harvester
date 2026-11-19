"""Safe ingestion events using existing stdlib handlers and request context."""

import hashlib
import json
import logging
import re
import sys
import textwrap
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from observability.diagnostics import caller_source, exception_fields, location_text, safe_location

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
    ignored: bool = False
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
    record = _event_fields(event, level, {**fields, "source": caller_source(2)})
    _logger.log(
        level,
        json.dumps(record, separators=(",", ":")),
        extra={"event": event, "event_fields": record},
        stacklevel=2,
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
        if key == "bq_location":
            record[key] = (
                identifier(value)
                if isinstance(value, str) and (
                    value in {"US", "EU", "GLOBAL"}
                    or re.fullmatch(r"[A-Za-z]+(?:-[A-Za-z]+)+[0-9]+", value)
                ) else "unrecognized"
            )
        elif key in {"source", "failure_site"}:
            location = safe_location(value)
            if location and not any(
                secret in str(location) for secret in (state.secrets if state else [])
            ):
                record[key] = location
        elif (key == "error_location" and isinstance(value, str)
              and value == location_text(fields.get("failure_site"))
              and not any(secret in value for secret in (state.secrets if state else []))):
            record[key] = value
        elif key in COUNTERS and (value is None or type(value) is int):
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
                "publication_pending",
                "publication_in_flight",
                "publication_unknown",
                "dependency_status",
            }
            and type(value) is int
        ):
            record[key] = value
        elif (
            key in {"verified", "allowlist_enforced", "authentication_required", "ledger_enabled"}
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
            "query_job_id",
            "dependency",
            "dependency_operation",
            "dependency_reason",
            "retry_classification",
        }:
            record[key] = identifier(value)
    return record


def error_fields(error: BaseException) -> dict[str, Any]:
    return exception_fields(error)


_STAGE_LABELS = {
    "configuration": "Apify",
    "authentication": "Auth",
    "event_validation": "Apify",
    "actor_authorization": "Apify",
    "actor_verification": "Apify",
    "dataset_verification": "Apify",
    "dataset_fetch": "Fetch",
    "normalization": "Normalize",
    "bigquery": "BigQuery",
    "recovery": "Recovery",
    "pubsub": "Pub/Sub",
}


def _compact_summary(fields: dict[str, object]) -> tuple[str, str]:
    """Present only trusted metadata; never render payloads or exception strings."""
    event = fields.get("event")
    label = _STAGE_LABELS.get(str(fields.get("stage")), "Ingestion")

    def count(key: str) -> object:
        value = fields.get(key)
        return value if type(value) is int else "unknown"

    if event == "ingestion.runtime.configured":
        flag = fields.get("ledger_enabled")
        ledger = "enabled" if flag is True else "disabled" if flag is False else "unknown"
        return "Config", f"BigQuery location={fields.get('bq_location', 'unknown')}; ledger={ledger}"
    if event == "ingestion.webhook.received":
        return "Apify", "Webhook received"
    if event == "ingestion.actor.verification_started":
        return "Apify", "Verifying run..."
    if event == "ingestion.dataset.fetch_started":
        return "Fetch", "Fetching dataset..."
    if event == "ingestion.normalization.started":
        return "Normalize", (
            f"Normalizing and deduplicating {count('received')} records..."
            if type(fields.get("received")) is int
            else "Normalizing and deduplicating records..."
        )
    if event == "ingestion.bigquery.started":
        return "BigQuery", (
            f"Checking and saving {count('unique')} unique jobs..."
            if type(fields.get("unique")) is int
            else "Checking and saving jobs..."
        )
    if event == "ingestion.recovery.started":
        return "Recovery", "Checking recovery..."
    if event == "ingestion.pubsub.started":
        return "Pub/Sub", "Publishing jobs..."
    if event == "ingestion.cancelled":
        summary = "Ingestion cancelled"
        if fields.get("persistence_outcome") == "unconfirmed":
            summary += "; persistence unconfirmed, stored row count unknown"
        elif fields.get("reason") == "publication_confirmation_incomplete":
            summary += "; publication confirmation incomplete"
        return label, summary
    if event == "ingestion.dataset.verified":
        return "Apify", (
            "Run and dataset verified"
            if fields.get("verified") is True
            else "Run verified; dataset verification incomplete"
        )
    if event == "ingestion.actor.aborted":
        return "Apify", "Run and dataset verified — ABORTED, partial source"
    if event == "ingestion.dataset.fetched":
        return "Fetch", f"{count('received')} records"
    if event == "ingestion.normalization.completed":
        summary = f"{count('unique')} unique, {count('in_batch_duplicates')} duplicates"
        if fields.get("rejected"):
            summary += f", {count('rejected')} rejected"
        return "Normalize", summary
    if event == "ingestion.bigquery.completed":
        return "BigQuery", f"{count('inserted')} inserted, {count('existing_duplicates')} existing"
    if event == "ingestion.recovery.completed":
        if fields.get("outcome") == "unconfirmed":
            return "Recovery", "Recovery unconfirmed"
        # Standalone reconciliation also emits publication counters.
        summary = f"{count('recovered')} jobs recovered"
        if "publish_failures" in fields:
            summary += f", {count('published')} published, {count('publish_failures')} failed"
        return "Recovery", summary
    if event == "ingestion.recovery.failed":
        return "Recovery", f"Recovery check failed: {fields.get('error_type') or 'unknown error'}"
    if event == "ingestion.pubsub.completed":
        return "Pub/Sub", f"{count('published')} published, {count('publish_failures')} failed"
    if event == "ingestion.publication.deferred":
        return "Pub/Sub", (
            f"Publication pending or held: {count('publication_pending')} pending, "
            f"{count('publication_in_flight')} in flight, {count('publication_unknown')} unknown"
        )
    if event == "ingestion.pubsub.publish_unconfirmed":
        return "Pub/Sub", f"Publication unconfirmed: {fields.get('error_type') or 'unknown error'}"
    if event == "ingestion.completed":
        outcome = fields.get("outcome")
        summary = {
            "success": "Ingestion successful",
            "empty": "No records to ingest",
            "partial": "Ingestion partially completed",
            "completed_with_rejections": (
                f"Ingestion completed with {count('rejected')} rejected records"
            ),
            "publication_unconfirmed": "Publication unconfirmed",
        }.get(str(outcome), f"Ingestion completed: {outcome or 'unknown outcome'}")
        return "Complete", summary
    if event == "ingestion.failed":
        if fields.get("persistence_outcome") == "unconfirmed":
            return "BigQuery", "Persistence unconfirmed; stored row count unknown"
        summary = (
            "Request rejected"
            if fields.get("error_category") == "request_rejected"
            else "Operation failed"
        )
        details = [str(fields[key]) for key in ("error_type",) if fields.get(key)]
        if fields.get("status_code") is not None:
            details.append(f"HTTP {fields['status_code']}")
        return label, summary + (f" ({', '.join(details)})" if details else "")
    if event == "ingestion.operation.retry":
        if fields.get("outcome") == "exhausted":
            return (
                label,
                f"Retries exhausted after {count('attempt')} attempts: {fields.get('error_type')}",
            )
        delay = fields.get("retry_delay_ms", 0)
        assert isinstance(delay, (int, float))
        return label, (
            f"Retry {count('attempt')}/{count('max_attempts')}: {fields.get('error_type')}"
            f"; next attempt in {delay / 1000:.2f}s"
        )
    if event == "ingestion.ignored":
        return "Apify", "Webhook ignored"
    details = [str(fields[key]) for key in ("reason", "outcome", "error_type") if fields.get(key)]
    return label, str(event) + (f" — {', '.join(details)}" if details else "")


class _CompactConsoleFilter(logging.Filter):
    """Hide routine milestones on this console handler only; retain all anomalies."""

    def filter(self, record: logging.LogRecord) -> bool:
        fields = getattr(record, "event_fields", None)
        if record.levelno >= logging.WARNING or not isinstance(fields, dict):
            return True
        if (
            fields.get("event") == "ingestion.dataset.verified"
            and fields.get("source_run_status") == "ABORTED"
        ):
            return False  # The subsequent warning combines verified provenance and partial source.
        return fields.get("event") not in {
            "ingestion.authentication.verified",
            "ingestion.authentication.skipped",
            "ingestion.event.validated",
            "ingestion.actor.authorized",
            "ingestion.actor.verified",
        }


class IngestionFormatter(logging.Formatter):
    """Render safe event fields; delegate unrelated records to their original formatter."""

    def __init__(
        self, fallback: logging.Formatter | None, *, development: bool, verbose: bool = False
    ):
        super().__init__()
        self.fallback = fallback or logging.Formatter()
        self.development = development
        self.verbose = verbose

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "event_fields", None)
        if record.name == "utils.retry":
            retry = getattr(record, "retry_fields", None)
            if isinstance(retry, dict):
                fields = _event_fields("ingestion.operation.retry", record.levelno, retry)
        if not isinstance(fields, dict):
            return self.fallback.format(record)
        if not self.development:
            return json.dumps(fields, separators=(",", ":"))
        timestamp = (
            datetime.fromisoformat(str(fields["timestamp"])).astimezone().strftime("%H:%M:%S")
        )
        severity = "WARN" if record.levelno == logging.WARNING else str(fields["severity"])
        if not self.verbose:
            label, summary = _compact_summary(fields)
            if (
                record.levelno >= logging.WARNING
                and fields.get("reason")
                and str(fields.get("event")).endswith((".started", "_started"))
            ):
                summary += f" — {fields['reason']}"
            timed_events = {
                "ingestion.dataset.fetched",
                "ingestion.normalization.completed",
                "ingestion.bigquery.completed",
                "ingestion.recovery.completed",
                "ingestion.pubsub.completed",
                "ingestion.completed",
                "ingestion.failed",
                "ingestion.ignored",
                "ingestion.cancelled",
            }
            if fields.get("event") in timed_events:
                duration = fields["duration_ms"]
                assert isinstance(duration, (int, float))
                summary += f" ({duration / 1000:.2f}s)"
            if record.levelno >= logging.WARNING and fields.get("request_id"):
                summary += f" [{str(fields['request_id'])[:8]}]"
            line = f"{timestamp} {severity:<5} [{label}] {summary}"
            if record.levelno >= logging.WARNING:
                diagnostic = []
                operation = fields.get("dependency_operation") or fields.get("operation")
                if operation:
                    diagnostic.append(str(operation))
                site = location_text(fields.get("failure_site")) or location_text(fields.get("source"))
                if site:
                    diagnostic.append(site)
                if fields.get("dependency_status") is not None:
                    diagnostic.append(f"HTTP {fields['dependency_status']}")
                if fields.get("dependency_reason"):
                    diagnostic.append(str(fields["dependency_reason"]))
                if fields.get("retry_classification"):
                    diagnostic.append(f"classification={fields['retry_classification']}")
                if diagnostic:
                    line += "\n" + textwrap.fill(
                        " | ".join(diagnostic), width=100, initial_indent="    ",
                        subsequent_indent="    ", break_long_words=False, break_on_hyphens=False,
                    )
            published = fields.get("published")
            if (
                fields.get("event") == "ingestion.completed"
                and type(published) is int
                and published > 0
                and fields.get("subscriber_processing") == "not_observed"
            ):
                line += f"\n{timestamp} INFO  [Worker] Processing not verified"
            return line
        details = "  ".join(
            f"{key}={json.dumps(value, ensure_ascii=True, separators=(',', ':'))}"
            for key, value in fields.items()
            if key not in {"timestamp", "severity", "message", "event"} and value is not None
        )
        wrapped = textwrap.fill(
            details,
            width=100,
            initial_indent="    ",
            subsequent_indent="    ",
            break_long_words=False,
            break_on_hyphens=False,
        )
        # StreamHandler supplies the final newline, leaving a blank line between events.
        return f"{timestamp} {severity:<5} {fields['event']}\n{wrapped}\n"


def configure_ingestion_logging(*, environment: str, level: str = "INFO") -> Callable[[], None]:
    """Wrap existing stream output once; return an idempotent restoration callback.

    INGESTION_LOG_LEVEL=DEBUG shows all events and safe fields without HTTP wire logs.
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
    console_filters = []
    for handler, formatter in original_formatters.items():
        development = environment.lower() == "development" and not isinstance(
            handler, logging.FileHandler
        )
        handler.setFormatter(
            IngestionFormatter(formatter, development=development, verbose=level == "DEBUG")
        )
        if development and level != "DEBUG":
            compact_filter = _CompactConsoleFilter()
            handler.addFilter(compact_filter)
            console_filters.append((handler, compact_filter))
    _logger.setLevel(level)
    for name in ("httpx", "httpcore"):
        target = logging.getLogger(name)
        original_levels[target] = target.level
        target.setLevel(logging.WARNING)

    def restore() -> None:
        global _restore_logging
        if _restore_logging is not restore:
            return
        for handler, compact_filter in console_filters:
            handler.removeFilter(compact_filter)
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
