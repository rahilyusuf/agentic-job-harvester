---
name: litellm-instructor
description: Modify structured LLM calls, Instructor validation, gateway aliases, proxy integration, or embedding access through the existing gateway. Use for model-access code; excludes switching providers or treating direct embeddings as an approved permanent design.
---

# Gateway and structured output

[Guardrails](../../rules/guardrails.md) G-1/G-2/G-3 own routing, privacy and validation.
[Stack guidance](../../rules/stack.md#model-configuration) references current configuration;
[architecture](../../../ARCHITECTURE.md#model-access) distinguishes actual implementation
from the proxy-only target. Do not maintain another provider/alias table here.

## Structured chat — Implemented interface

Read [gateway/client.py](../../../gateway/client.py) and [agents/base.py](../../../agents/base.py).
`get_instructor_client()` returns `GatewayClient`, whose `create(model, response_model,
messages, *, trace_id=None, max_retries=3, **kwargs)` delegates to Instructor's wrapped
LiteLLM completion. Agents use `_call_llm()` through their shared base. The wrapper adds
trace metadata; using its exposed `.chat` property directly bypasses that wrapper logic.

Set `MODEL_ALIAS` to a registered chat alias in
[gateway/litellm_config.yaml](../../../gateway/litellm_config.yaml), not a provider model
string. Preserve the existing router/scoring/reflection allocation. Treat retries as
cost-bearing requests, and validate final Pydantic output before use/persistence.
Do not parse arbitrary returned JSON or add direct SDK chat calls as a fallback.

## Embeddings — current deviation, decision outstanding

`generate_embedding(text)` currently calls direct Vertex prediction via an executor,
using `text-embedding-004` and a 768-value result check. There is no configured proxy
embedding alias. G-1's proxy endpoint requirement remains the target; A-1 is unresolved,
not a permanent exception. Do not route raw embeddings through Instructor's chat API.
Keep all callers using the existing helper; do not copy the provider adapter elsewhere.

For any separately authorized routing change, preserve model/provider and task/input
compatibility, dimensions, region behavior, privacy, retries and stored-vector semantics.
Read worker/profile callers: job input formatting is not the profile input contract.
Do not switch providers or regenerate the corpus merely to align with documentation.

## Verification and responsibilities

Verify traffic reaches the intended proxy with a mock/local endpoint and explicit
request assertions; LiteLLM global settings alone do not prove proxy traversal.
Inspect alias mappings, fallback and rate configuration independently of actual
runtime enforcement. Use [langfuse-tracing](../langfuse-tracing/SKILL.md) for privacy-safe
instrumentation; do not log prompts to debug gateway routing.

Use mocked client/provider adapters to check model selection, response validation,
retry limits, metadata propagation, malformed outputs and embedding dimensions/failure
behavior. Instructor retries and provider fallbacks must remain bounded and observable
without sensitive capture. Live model/cloud calls require separate authorization.
