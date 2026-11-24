"""Privacy-safe Langfuse 4 observations; telemetry never retries business work.

Agent decorators retain their names but capture no arguments, results or exception
content. Gateway generations represent logical Instructor invocations, not provider
attempts or verified usage/cost. No client or exporter is constructed at import.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
from contextlib import contextmanager
from functools import wraps
from time import perf_counter
from typing import Any

import litellm
from langfuse import Langfuse, get_client

from config.settings import get_settings

logger = logging.getLogger(__name__)

__all__ = [
    "setup_observability", "flush_observability", "get_langfuse", "observe",
    "update_current_span", "score_trace", "safe_observation",
]
_AGENT_NAMES = frozenset({
    "JobIntakeRouter", "ScorerAgent", "SkepticAgent", "CompanyResearchAgent",
    "CompanyVerificationAgent", "RedFlagDetectorAgent", "SkillGapAgent", "ReflectionAgent",
})
_MODEL_ALIASES = frozenset({"router-flash", "fast-flash", "deep-pro"})


def _failure(error: BaseException) -> None:
    try:
        logger.warning("Langfuse operation failed (error_type=%s)", type(error).__name__)
    except (Exception, asyncio.CancelledError):
        pass  # A broken logging handler must not turn telemetry failure into business failure.


def remove_langfuse_callbacks() -> None:
    """Remove only owned callback names, retaining unrelated callback objects/order."""
    for name in ("callbacks", "success_callback", "failure_callback"):
        callbacks = getattr(litellm, name, None)
        if isinstance(callbacks, list):
            callbacks[:] = [
                callback for callback in callbacks
                if not (isinstance(callback, str) and callback in {"langfuse", "langfuse_otel"})
            ]


def setup_observability() -> None:
    """Configure lazily; model routing and unrelated callbacks remain untouched."""
    remove_langfuse_callbacks()
    try:
        settings = get_settings()
        os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
        os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
        os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_host)
    except Exception as error:
        _failure(error)


def get_langfuse() -> Langfuse | None:
    """Check disablement on every call; the SDK owns its existing client cache."""
    if os.environ.get("LANGFUSE_TRACING_ENABLED", "true").lower() == "false":
        return None
    return get_client()


def _safe_metadata(metadata: dict | None) -> dict:
    """Deny unknown/free-text values; identifiers are never exported verbatim."""
    result = {}
    for key, value in (metadata or {}).items():
        if key == "operation" and value in ("agent", "structured_completion", "feedback"):
            result[key] = value
        elif key == "outcome" and value in ("success", "error", "cancelled"):
            result[key] = value
        elif key == "agent_name" and isinstance(value, str) and value in _AGENT_NAMES:
            result[key] = value
        elif key == "model_alias" and isinstance(value, str) and value in _MODEL_ALIASES:
            result[key] = value
        elif key in ("duration_ms", "retry_limit") and type(value) is int and value >= 0:
            result[key] = value
        elif key in ("job_id", "user_id", "correlation_id") and isinstance(value, str):
            result[key + "_hash"] = hashlib.sha256(value.encode()).hexdigest()
    return result


@contextmanager
def safe_observation(*, name: str, generation: bool = False, metadata: dict | None = None):
    """Keep native parenting; never send a business exception to SDK __exit__.

    The SDK owns context attachment and unwinds it on entry and end failures.
    No fallback ever re-executes the code inside this context manager.
    """
    manager = observation = None
    started = perf_counter()
    safe = {}
    try:
        safe = _safe_metadata(metadata)
        client = get_langfuse()
        if client is not None:
            options: dict[str, Any] = {
                "name": "structured_completion" if generation else name if name in _AGENT_NAMES else "agent",
                "as_type": "generation" if generation else "span",
                "input": None, "output": None, "metadata": safe,
            }
            if generation and safe.get("model_alias"):
                options["model"] = safe["model_alias"]
            candidate = client.start_as_current_observation(**options)
            observation = candidate.__enter__()
            manager = candidate
    except (Exception, asyncio.CancelledError) as error:
        _failure(error)
    outcome = "success"
    try:
        yield observation
    except BaseException as error:
        outcome = "cancelled" if isinstance(error, asyncio.CancelledError) else "error"
        raise
    finally:
        if observation is not None:
            try:
                observation.update(metadata={
                    **safe, "outcome": outcome,
                    "duration_ms": max(0, round((perf_counter() - started) * 1000)),
                })
            except (Exception, asyncio.CancelledError) as error:
                _failure(error)
        if manager is not None:
            try:
                manager.__exit__(None, None, None)
            except (Exception, asyncio.CancelledError) as error:
                _failure(error)


def observe(func=None, *, name=None, capture_input=False, capture_output=False):
    """Small sync/async adapter for existing agent decorators; capture stays off.

    Capture arguments are accepted for compatibility, but cannot enable sensitive
    capture. Generators are not used by agent entry points and are not instrumented.
    """
    def decorate(function):
        span_name = name if name in _AGENT_NAMES else "agent"
        metadata = {"operation": "agent", "agent_name": span_name}
        if inspect.iscoroutinefunction(function):
            @wraps(function)
            async def async_call(*args, **kwargs):
                with safe_observation(name=span_name, metadata=metadata):
                    return await function(*args, **kwargs)
            return async_call

        @wraps(function)
        def sync_call(*args, **kwargs):
            with safe_observation(name=span_name, metadata=metadata):
                return function(*args, **kwargs)
        return sync_call
    return decorate(func) if func is not None else decorate


def flush_observability() -> None:
    try:
        client = get_langfuse()
        if client is not None:
            client.flush()
    except Exception as error:
        _failure(error)


def update_current_span(*, user_id=None, session_id=None, metadata=None) -> None:
    try:
        safe = _safe_metadata({
            **(metadata or {}), "user_id": user_id, "correlation_id": session_id,
        })
        client = get_langfuse()
        if client is not None and safe:
            client.update_current_span(metadata=safe)
    except Exception as error:
        _failure(error)


def score_trace(trace_id, job_id, user_action, *, comment=None) -> None:
    """Best-effort feedback. Existing numeric labels remain unchanged for P0."""
    try:
        scores = {"apply": 1.0, "pass": 0.0, "analyze": 0.5}
        action = user_action.lower()
        client = get_langfuse()
        if client is not None:
            client.create_score(
                trace_id=trace_id, name="user_acceptance", value=scores.get(action, 0.5),
                comment="action=" + (action if action in scores else "unknown"),
            )
    except Exception as error:
        _failure(error)
