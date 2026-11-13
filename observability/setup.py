"""observability/setup.py — LangFuse init, @observe decorator, and trace scoring.

Initialises LangFuse once per process. Provides:
  - ``setup_observability()``  — call at service startup
  - ``get_langfuse()``         — singleton LangFuse client
  - ``observe``                — re-export of langfuse.decorators.observe
  - ``score_trace()``          — emit user_acceptance score on HITL action

Every agent must decorate its run method with @observe(name="AgentName") so
prompts, tokens, and latency are captured without manual instrumentation.
"""

from __future__ import annotations

import logging
from functools import lru_cache

import litellm
from langfuse import Langfuse
from langfuse.decorators import observe  # re-exported for convenience

from config.settings import get_settings

logger = logging.getLogger(__name__)

__all__ = ["setup_observability", "get_langfuse", "observe", "score_trace"]


def setup_observability() -> None:
    """Initialise LangFuse and wire it as a LiteLLM callback.

    Call this at the top of every Cloud Run service entry point (main / lifespan).
    Idempotent — safe to call multiple times.
    """
    settings = get_settings()

    # Initialise the LangFuse SDK (sets global env vars internally)
    lf = get_langfuse()

    # Wire LiteLLM to auto-log via LangFuse
    # This complements the proxy-side callback in litellm_config.yaml;
    # ensures local/test runs without the proxy also emit traces.
    if "langfuse" not in litellm.success_callback:
        litellm.success_callback.append("langfuse")
    if "langfuse" not in litellm.failure_callback:
        litellm.failure_callback.append("langfuse")

    # Set LiteLLM metadata keys for LangFuse
    import os
    os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
    os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
    os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_host)

    logger.info("Observability initialised (LangFuse host=%s)", settings.langfuse_host)


@lru_cache(maxsize=1)
def get_langfuse() -> Langfuse:
    """Return the singleton LangFuse client.

    Reads LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY, and LANGFUSE_HOST from
    the Settings object / environment.
    """
    settings = get_settings()
    return Langfuse(
        public_key=settings.langfuse_public_key,
        secret_key=settings.langfuse_secret_key,
        host=settings.langfuse_host,
    )


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
        user_action: One of "apply" | "pass" | "analyze".
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

    lf.score(
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
