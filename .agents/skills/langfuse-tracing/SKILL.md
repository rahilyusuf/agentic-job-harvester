---
name: langfuse-tracing
description: Modify or debug Langfuse instrumentation, privacy-safe capture, trace correlation, callbacks, shutdown flushing, or delayed HITL feedback. Use for telemetry work; do not enable sensitive prompt logging to troubleshoot ordinary scoring.
---

# Langfuse tracing and feedback

[Guardrails](../../rules/guardrails.md) G-2/G-9 own privacy and correlation requirements;
[architecture](../../../ARCHITECTURE.md#observability-and-reflection) owns status/evidence.
This skill owns SDK integration procedures, not model routing, user authorization or
an independent acceptance-label policy.

## Current implementation — inspect before changing

[observability/setup.py](../../../observability/setup.py) re-exports `observe`, provides
`setup_observability()`, `flush_observability()`, `get_langfuse()`,
`update_current_span()` and `score_trace()`. Agent public methods use decorators;
there is no `_invoke_llm` method to decorate. Setup uses the imported SDK's
`get_client()`/`create_score()` API. Comments claiming an installed version do not prove
compatibility with the broadly ranged manifest or deployed environment.

[gateway/client.py](../../../gateway/client.py) passes trace metadata and registers
`langfuse` callbacks; setup appends `langfuse_otel`; the
[proxy config](../../../gateway/litellm_config.yaml) also declares callbacks. This can
produce conflicting/duplicate capture. Telemetry privacy, exact SDK linkage and shutdown
coverage remain **Unverified**; S-5/S-9 and A-5 are open in the
[backlog](../../rules/remediation-backlog.md).

## Procedure for authorized telemetry work

- Inspect supported SDK and proxy callback behavior before selecting an API. Use one
  intentional strategy for client/proxy spans; do not append callbacks blindly or
  replace instrumentation merely to match an old example.
- Audit decorator inputs/outputs, callback bodies, retries and exception logging against
  G-2. Default to safe IDs, status, model alias, timing, tokens and bounded non-sensitive
  metadata. In-memory resume summaries remain sensitive. Redaction must happen before
  capture; model-bound text is not automatically safe for telemetry.
- Establish an actual SDK trace context and propagate its supported ID; UUID metadata
  alone is not proof. Persist trusted job/user/trace association for delayed feedback.
  The current evaluation schema lacks a trace field; adding one requires separate
  schema/migration work, not a fictitious session key in documentation.
- Keep authorization and durable action writes independent of telemetry availability.
  G-9 defines acceptance semantics; the current Analyze=0.5 implementation conflicts
  with that target and must not be presented as a closed finding.
- Initialize at service startup and flush on shutdown using supported lifecycle hooks.
  The helper exists, but not all services call it. Avoid capture failure masking a
  successful business write or triggering duplicate business work.

## Validation

Use disabled/mocked telemetry with synthetic inputs to inspect captured payloads,
callback count, trace continuity, lifecycle flushing, retries, and SDK failure behavior.
Check that delayed apply/pass attaches to the original owned evaluation and Analyze
is recorded separately under the target contract. Never send a real resume or credential
to Langfuse as a validation fixture. No live telemetry calls are needed for doc checks.
