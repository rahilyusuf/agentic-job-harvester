"""gateway/client.py — AsyncInstructor client + embeddings helper.

THE SINGLE AUTHORISED ENTRY POINT for all LLM and embedding calls.

Usage in agents:
    from gateway.client import get_instructor_client, generate_embedding

    client = get_instructor_client()
    result = await client.chat.completions.create(
        model="fast-flash",
        response_model=MySchema,
        messages=[...],
        metadata={"trace_id": trace_id},
    )

    embedding = await generate_embedding("title company location description[:2000]")
"""

from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from typing import Any

import instructor
import litellm
from google.cloud import aiplatform
from pydantic import BaseModel

from config.settings import get_settings

logger = logging.getLogger(__name__)

# ── LiteLLM global setup ───────────────────────────────────────────────────────


def _configure_litellm() -> None:
    """Configure LiteLLM to route through the proxy with LangFuse callbacks."""
    settings = get_settings()
    litellm.api_base = settings.litellm_proxy_base_url
    litellm.api_key = settings.litellm_proxy_api_key
    # LangFuse callback is already wired in litellm_config.yaml on the proxy side;
    # this client-side registration ensures local/test runs also emit traces.
    litellm.success_callback = ["langfuse"]
    litellm.failure_callback = ["langfuse"]


# Initialise once at import time
_litellm_configured = False


def _ensure_litellm_configured() -> None:
    global _litellm_configured  # noqa: PLW0603
    if not _litellm_configured:
        _configure_litellm()
        _litellm_configured = True


# ── Instructor client factory ──────────────────────────────────────────────────


class GatewayClient:
    """Thin wrapper around an AsyncInstructor client bound to the LiteLLM proxy.

    Always use ``get_instructor_client()`` rather than instantiating directly
    to benefit from connection pooling and singleton semantics.
    """

    def __init__(self) -> None:
        _ensure_litellm_configured()
        self._client = instructor.from_litellm(litellm.acompletion)

    @property
    def chat(self) -> Any:
        return self._client.chat

    async def create(
        self,
        model: str,
        response_model: type[BaseModel],
        messages: list[dict[str, str]],
        *,
        trace_id: str | None = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> BaseModel:
        """Create a structured completion via Instructor.

        Args:
            model: LiteLLM model alias (e.g. "fast-flash", "deep-pro").
            response_model: Pydantic v2 model class for structured output.
            messages: OpenAI-style message list.
            trace_id: LangFuse trace ID for correlation. Passed as LiteLLM metadata.
            max_retries: Instructor retry count on validation failure.
            **kwargs: Additional kwargs forwarded to litellm.acompletion.

        Returns:
            Validated instance of response_model.
        """
        metadata: dict[str, Any] = kwargs.pop("metadata", {})
        if trace_id:
            metadata["trace_id"] = trace_id

        return await self._client.chat.completions.create(
            model=model,
            response_model=response_model,
            messages=messages,
            max_retries=max_retries,
            metadata=metadata,
            **kwargs,
        )


@lru_cache(maxsize=1)
def get_instructor_client() -> GatewayClient:
    """Return the singleton GatewayClient.

    Call ``get_instructor_client.cache_clear()`` in tests to get a fresh instance.
    """
    return GatewayClient()


# ── Embedding helper ───────────────────────────────────────────────────────────

_EMBEDDING_MODEL = "text-embedding-004"
_EMBEDDING_DIMENSIONS = 768


async def generate_embedding(text: str) -> list[float]:
    """Generate a 768-dim embedding using text-embedding-004.

    Input must follow the contract:
        f"{title} {company} {location} {description[:2000]}"

    This is the ONLY place text-embedding-004 is called in the project.
    All embedding generation routes through here.

    Args:
        text: Pre-formatted text string (title + company + location + description[:2000]).

    Returns:
        768-dimensional float list suitable for BigQuery FLOAT64 REPEATED column.
    """
    settings = get_settings()

    # Initialise Vertex AI for the embedding call
    aiplatform.init(project=settings.gcp_project_id, location=settings.gcp_region)

    # Run in thread executor to avoid blocking the event loop
    loop = asyncio.get_event_loop()
    embedding = await loop.run_in_executor(None, _blocking_embed, text, settings.gcp_project_id)
    return embedding


def _blocking_embed(text: str, project_id: str) -> list[float]:
    """Blocking embedding call — run via executor inside generate_embedding()."""
    from google.cloud.aiplatform_v1beta1 import PredictionServiceClient
    from google.cloud.aiplatform_v1beta1.types import PredictRequest
    from google.protobuf import struct_pb2

    client = PredictionServiceClient(
        client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"}
    )
    endpoint = (
        f"projects/{project_id}/locations/us-central1"
        f"/publishers/google/models/{_EMBEDDING_MODEL}"
    )

    instance = struct_pb2.Value()
    instance.struct_value.fields["content"].string_value = text[:10000]  # hard cap

    request = PredictRequest(endpoint=endpoint, instances=[instance])
    response = client.predict(request=request)

    values: list[float] = list(response.predictions[0].struct_value.fields["embeddings"]
                                .struct_value.fields["values"].list_value.values)
    values_float = [v.number_value for v in values]

    if len(values_float) != _EMBEDDING_DIMENSIONS:
        raise ValueError(
            f"Expected {_EMBEDDING_DIMENSIONS}-dim embedding, got {len(values_float)}"
        )
    return values_float
