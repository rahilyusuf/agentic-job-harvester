---
name: litellm-instructor
description: Guidance on routing all LLM chat completions and embeddings through the LiteLLM Proxy using Instructor for Pydantic v2 schema enforcement. Covers model aliases (fast-flash, deep-pro, router-flash), LangFuse trace metadata injection, and zero-direct-API rules.
---

# LiteLLM Proxy + Instructor Structured Output Patterns

This skill guides all LLM invocations, model alias mapping, schema enforcement, and embedding requests across `agentic-job-harvester-v2`.

---

## 1. Core Principle: Zero Direct LLM Calls (Guardrail G-1)

Every Gemini Flash, Gemini Pro, and text embedding call MUST route through the LiteLLM Proxy via `gateway/client.py`:
- ❌ **Forbidden:** `genai.Client()`, `google.generativeai`, `vertexai.generative_models.GenerativeModel`.
- ❌ **Forbidden:** Direct unproxied calls to Google APIs from agents.
- ✅ **Required:** `from gateway.client import get_instructor_client, generate_embedding`.

---

## 2. Gateway Client (`gateway/client.py`)

`gateway/client.py` is the single authorized gateway module in the codebase:

```python
from gateway.client import get_instructor_client, generate_embedding

# Structured chat completions with Instructor
client = get_instructor_client()
result = await client.chat.completions.create(
    model="fast-flash",
    response_model=MyOutputSchema,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ],
    metadata={"trace_id": langfuse_trace_id},
)

# Pre-computed 768-dim embeddings
vector = await generate_embedding("Senior AI Engineer DeepMind London ...")
```

---

## 3. Standard Model Aliases

Always use the registered LiteLLM aliases instead of full provider model strings:

| Alias | Target Model | Max Tokens / RPM | Component Usage |
|---|---|---|---|
| `fast-flash` | `vertex_ai/gemini-2.5-flash` | 4096 / standard | `ScorerAgent`, `SkepticAgent`, `CompanyVerificationAgent`, `RedFlagDetectorAgent`, `SkillGapAgent` |
| `router-flash` | `vertex_ai/gemini-2.5-flash` | 512 / standard | `JobIntakeRouter` (minimal context, fast routing) |
| `deep-pro` | `vertex_ai/gemini-2.5-pro` | 8192 / 10 RPM cap | `ReflectionAgent` (weekly offline synthesis) |
| `text-embedding-004` | `vertex_ai/text-embedding-004` | 768 dims | `gateway/client.py::generate_embedding()` |

---

## 4. Schema Enforcement (Guardrail G-3)

- All agent outputs must be strictly typed Pydantic v2 schemas located in `schemas/`.
- Unvalidated dictionary returns from any agent are strictly forbidden.
- `instructor.from_litellm()` automatically inspects the `response_model`, passes JSON schema parameters to the model, and retries validation failures up to `max_retries` with the validation error context injected into the conversation.

---

## 5. Telemetry & Observability Injection (Guardrail G-9)

Every LLM completion call must inject LangFuse correlation metadata:
```python
metadata = {
    "trace_id": trace_id,
    "user_id": user_id,
    "session_id": session_id,
}
```
LiteLLM is pre-configured with LangFuse callbacks (`litellm.success_callback = ["langfuse_otel"]`), ensuring token counts, latency, and prompt traces are automatically recorded and linked to user actions.
