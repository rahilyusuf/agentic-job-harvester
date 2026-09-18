"""observability/setup.py — LangFuse v4 init, @observe decorator, and trace scoring.

Initialises LangFuse once per process (via environment variables). Provides:
  - ``setup_observability()``    — call at service startup
  - ``flush_observability()``    — call at service shutdown to drain the buffer
  - ``get_langfuse()``           — singleton LangFuse v4 client
  - ``observe``                  — re-export of langfuse.observe (v4 top-level)
  - ``update_current_span``      — re-export for setting user_id / session_id
                                   on the active span from within agent code
  - ``score_trace()``            — emit user_acceptance score on HITL action

Every agent must decorate its public run method with @observe(name="AgentName")
so prompts, tokens, and latency are captured without manual instrumentation.

LangFuse v4 API notes (SDK 4.x, installed: 4.15.2):
  - ``from langfuse import observe, get_client`` — no langfuse.decorators module
  - Client is initialised via LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_HOST
    env vars only; passing credentials to Langfuse() constructor is no longer supported.
  - ``create_score()`` replaces the old ``score()`` method.
  - ``client.update_current_span()`` replaces langfuse_context.update_current_trace().
"""

from __future__ import annotations

import logging
from functools import lru_cache

import litellm
import langfuse
from langfuse import get_client, observe  # re-exported for convenience
from langfuse import Langfuse

from config.settings import get_settings

logger = logging.getLogger(__name__)

__all__ = [
    "setup_observability",
    "flush_observability",
    "get_langfuse",
    "observe",
    "update_current_span",
    "score_trace",
]


def setup_observability() -> None:
    """Initialise LangFuse and wire it as a LiteLLM callback.

    Call this at the top of every Cloud Run service entry point (main / lifespan).
    Idempotent — safe to call multiple times.

    LangFuse v4 reads credentials exclusively from environment variables:
      LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY, LANGFUSE_HOST
    """
    import os

    settings = get_settings()

    # Set LangFuse env vars before the SDK auto-initialises
    os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
    os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
    os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_host)

    # Wire LiteLLM to auto-log via LangFuse
    # This complements the proxy-side callback in litellm_config.yaml;
    # ensures local/test runs without the proxy also emit traces.
    if "langfuse" not in litellm.success_callback:
        litellm.success_callback.append("langfuse")
    if "langfuse" not in litellm.failure_callback:
        litellm.failure_callback.append("langfuse")

    logger.info("Observability initialised (LangFuse host=%s)", settings.langfuse_host)


def flush_observability() -> None:
    """Flush all pending LangFuse events to the backend.

    Call this in every service's shutdown hook (lifespan ``finally`` / ``atexit``).
    Without an explicit flush, traces buffered in memory are lost when the
    Cloud Run container exits after the SIGTERM grace period.
    """
    try:
        get_langfuse().flush()
        logger.info("LangFuse flush complete")
    except Exception:  # noqa: BLE001
        logger.warning("LangFuse flush failed — some traces may be lost", exc_info=True)


@lru_cache(maxsize=1)
def get_langfuse() -> Langfuse:
    """Return the singleton LangFuse v4 client.

    LangFuse v4 reads credentials from LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY
    / LANGFUSE_HOST env vars set by setup_observability() at startup.
    """
    return get_client()


def update_current_span(
    *,
    user_id: str | None = None,
    session_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Enrich the currently active LangFuse span with trace-level context.

    Wraps ``get_client().update_current_span()`` with a safe no-op if there is
    no active span (e.g. during unit tests without a live LangFuse connection).

    Args:
        user_id:    Candidate user ID — makes traces filterable per user in the UI.
        session_id: Job ID — groups all agent spans for the same job in Sessions view.
        metadata:   Arbitrary extra key-value pairs to attach to the span.
    """
    try:
        extra_meta: dict = metadata or {}
        if user_id:
            extra_meta["user_id"] = user_id
        if session_id:
            extra_meta["session_id"] = session_id
        if extra_meta:
            get_client().update_current_span(metadata=extra_meta)
    except Exception:  # noqa: BLE001
        # Never let observability failures propagate into agent logic.
        logger.debug("update_current_span skipped (no active span or client disabled)")


def score_trace(
    trace_id: str,
    job_id: str,
    user_action: str,
    *,
    comment: str | None = None,
) -> None:
    """Emit a ``user_acceptance`` score on a LangFuse trace.

    Called by the HITL controller when the user clicks ✅ Apply or 👎 Pass.

    Args:
        trace_id: The LangFuse trace ID created during Tier 1 scoring.
        job_id: The job_id for audit trail reference.
        user_action: One of \"apply\" | \"pass\" | \"analyze\".
        comment: Optional free-text comment from the user.
    """
    lf = get_langfuse()

    # Map user actions to numeric acceptance score
    action_scores: dict[str, float] = {
        "apply": 1.0,
        "pass": 0.0,
        "analyze": 0.5,  # neutral — user wants more info
    }
    score_value = action_scores.get(user_action.lower(), 0.5)

    # v4 API: create_score() replaces the old score() method
    lf.create_score(
        trace_id=trace_id,
        name="user_acceptance",
        value=score_value,
        comment=comment or f"job_id={job_id} action={user_action}",
    )
    logger.info(
        "LangFuse score emitted: trace_id=%s job_id=%s action=%s score=%.1f",
        trace_id,
        job_id,
        user_action,
        score_value,
    )
