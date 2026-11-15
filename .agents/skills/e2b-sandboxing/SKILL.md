---
name: e2b-sandboxing
description: Modify or validate generated Python challenge execution through the E2B sandbox tool, including deadlines, cleanup, bounded output, and typed results. Excludes running generated code locally or enabling Tier 2 automatically.
---

# E2B challenge execution

[Guardrails](../../rules/guardrails.md) G-6 own isolation, deadline and output limits;
G-5 owns Tier 2 authorization. This skill owns sandbox integration, not eligibility
or model selection. See [architecture](../../../ARCHITECTURE.md#tier-2-and-research)
for Implemented/Planned/Unverified status.

## Current integration — Implemented source

[SkillGapAgent](../../../agents/skillgap_agent.py) identifies gaps, generates challenges
for blocking gaps and delegates to [tools/e2b_tools.py](../../../tools/e2b_tools.py).
The tool imports `AsyncSandbox` from `e2b_code_interpreter`, uses a context manager,
and returns [CodeChallenge](../../../schemas/diligence_models.py). Its fields are
`skill`, `challenge_description`, `generated_code`, `sandbox_stdout`, `sandbox_stderr`,
`execution_success`, `execution_time_ms` and `sandbox_id`; do not use old example fields
such as `passed`, `python_code`, or `stdout` in this model.

`execute_in_sandbox(skill, challenge_description, python_code)` is the tool interface.
The input argument name differs intentionally from the model's `generated_code` field.
The current tool reads `settings.e2b_sandbox_timeout_seconds`; there is no standalone
timeout constant. Settings allow values beyond G-6, an unresolved S-10 deviation.

## Implementation procedure

Read the actual tool, schema and supported SDK contract before changing lifecycle
calls. The broad dependency range does not verify that a copied constructor/context
manager example is supported. Keep the credential inside the adapter; generated code
must not receive application secrets, resume text or cloud credentials.

Distinguish sandbox lifetime, network request timeout and code execution deadline.
Implement G-6's execution bound and cleanup for success, timeout, error and cancellation
when that work is authorized. Truncating persisted output is not a memory limit on the
SDK response; evaluate both. Preserve safe typed failure results without returning
secret-bearing exception strings. Review current success detection, which treats stderr
as failure; stderr output alone need not imply failed execution.

## Validation

Mock sandbox creation/execution/cleanup and cover oversized output, assertions/runtime
errors, creation failure, timeout, cancellation and schema validation. Validate exact
SDK behavior in a separately authorized integration environment. Do not execute generated
challenges locally as a shortcut, or call E2B during documentation checks. Failure cases
must remain usable `CodeChallenge` results without violating field limits.
