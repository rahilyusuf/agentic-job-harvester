"""agents/base.py — BaseIntelligenceAgent.

The abstract base class for all agents in this system. Wires together:
  1. Instructor + LiteLLM proxy (via gateway/client.py)
  2. LangFuse @observe tracing (via observability/setup.py)
  3. Pydantic v2 output validation contract

Subclasses MUST:
  - Define ``MODEL_ALIAS``: the LiteLLM model alias (e.g. "fast-flash")
  - Define ``OUTPUT_SCHEMA``: a Pydantic BaseModel subclass
  - Implement ``_build_messages()``: returns the prompt messages list
  - Optionally override ``_system_prompt()``: the system instruction

Subclasses MUST NOT:
  - Call LiteLLM / Vertex AI / Google GenAI directly
  - Return unvalidated dict outputs
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel

from gateway.client import GatewayClient, get_instructor_client
from observability.setup import observe

logger = logging.getLogger(__name__)

TOutput = TypeVar("TOutput", bound=BaseModel)


class BaseIntelligenceAgent(ABC):
    """Abstract base for all Instructor + LiteLLM-backed agents.

    Provides:
      - Structured LLM calls via ``_call_llm()``
      - Automatic LangFuse tracing via @observe on subclass run() methods
      - Consistent trace_id propagation for HITL score correlation

    Usage pattern:
        class MyAgent(BaseIntelligenceAgent):
            MODEL_ALIAS = "fast-flash"
            OUTPUT_SCHEMA = MyOutputSchema

            async def run(self, job: RawJob, user_id: str) -> MyOutputSchema:
                return await self._call_llm(
                    messages=self._build_messages(job),
                    trace_id=self._new_trace_id(),
                )
    """

    #: LiteLLM model alias — must match a key in gateway/litellm_config.yaml
    MODEL_ALIAS: ClassVar[str]
    #: Pydantic v2 model class for structured output enforcement
    OUTPUT_SCHEMA: ClassVar[type[BaseModel]]

    def __init__(self, client: GatewayClient | None = None) -> None:
        self._client = client or get_instructor_client()
        self._logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def _system_prompt(self) -> str:
        """Return the system-level instruction for this agent."""
        ...

    @abstractmethod
    def _build_messages(self, *args: Any, **kwargs: Any) -> list[dict[str, str]]:
        """Build the OpenAI-style messages list for a specific invocation."""
        ...

    async def _call_llm(
        self,
        messages: list[dict[str, str]],
        *,
        trace_id: str | None = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> BaseModel:
        """Execute a structured LLM call and return a validated Pydantic instance.

        Args:
            messages: OpenAI-format message list (system + user turns).
            trace_id: LangFuse trace ID for cross-trace correlation.
            max_retries: Instructor auto-retry count on validation failure.
            **kwargs: Additional kwargs forwarded to LiteLLM (e.g. temperature override).

        Returns:
            Validated instance of self.OUTPUT_SCHEMA.
        """
        if not hasattr(self, "MODEL_ALIAS"):
            raise NotImplementedError(
                f"{self.__class__.__name__} must define MODEL_ALIAS class variable."
            )
        if not hasattr(self, "OUTPUT_SCHEMA"):
            raise NotImplementedError(
                f"{self.__class__.__name__} must define OUTPUT_SCHEMA class variable."
            )

        tid = trace_id or self._new_trace_id()
        self._logger.debug(
            "Calling %s model=%s trace_id=%s", self.__class__.__name__, self.MODEL_ALIAS, tid
        )

        result = await self._client.create(
            model=self.MODEL_ALIAS,
            response_model=self.OUTPUT_SCHEMA,  # type: ignore[arg-type]
            messages=messages,
            trace_id=tid,
            max_retries=max_retries,
            **kwargs,
        )
        return result

    @staticmethod
    def _new_trace_id() -> str:
        """Generate a new unique trace ID for LangFuse correlation."""
        return str(uuid.uuid4())

    def _system_message(self) -> dict[str, str]:
        """Return the system message dict."""
        return {"role": "system", "content": self._system_prompt()}

    def _user_message(self, content: str) -> dict[str, str]:
        """Return a user message dict."""
        return {"role": "user", "content": content}
