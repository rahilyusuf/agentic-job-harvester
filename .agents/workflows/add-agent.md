# Add an agent to the current Python pipeline

Use only for an explicitly authorized agent feature. Read [AGENTS.md](../../AGENTS.md),
[guardrails](../rules/guardrails.md), [conventions](../rules/conventions.md) and the
[pipeline skill](../skills/pipeline-orchestration/SKILL.md). This workflow does not
approve ADK integration, a new scoring path, automatic Tier 2, or a provider change.

## Prepare the contract

1. Identify the use case, stage, consent/cost boundary and existing inputs. Read
   [architecture contracts](../../ARCHITECTURE.md#function-and-data-contracts) and
   [repositories/interfaces.py](../../repositories/interfaces.py). Check relevant
   [backlog](../rules/remediation-backlog.md) items before relying on missing controls.
2. Define or reuse a Pydantic output model in `schemas/` with validation constraints.
   Distinguish model output from orchestration results/transport serialization. Explicit
   job/user/trace arguments replace historical session-key/output-key conventions.
3. Specify agent and repository dependencies. Keep concrete construction in composition
   entry points. If the feature needs a new schema, state transition, table or boundary,
   include its compatibility/migration design in review; do not implement unrelated work.

## Implement within authorized scope

4. Follow `BaseIntelligenceAgent`: `MODEL_ALIAS`, `OUTPUT_SCHEMA`, `_system_prompt()`,
   `_build_messages()` and an async public operation. Check the
   [gateway skill](../skills/litellm-instructor/SKILL.md) for the supported wrapper and
   configuration-owned aliases. Do not copy provider SDK calls.
5. Trace external prompt inputs using the [sanitization skill](../skills/input-sanitization/SKILL.md)
   and instrument safely using [tracing guidance](../skills/langfuse-tracing/SKILL.md).
   Preserve G-2 privacy throughout retries, exceptions and output handling.
6. Wire only the approved pipeline stage, validate outputs before use/persistence, and
   preserve bounded execution, short-circuiting and error semantics. If generated code
   is involved, use the [sandbox skill](../skills/e2b-sandboxing/SKILL.md).

## Verify and document

7. Use mocked gateway/repository/tool clients to exercise valid and invalid outputs,
   call boundaries, cost/consent controls and failure behavior. Preserve existing
   pipeline and ingestion regressions. No live model/cloud calls are a default unit check.
8. Update architecture and structure documentation with accurate status/evidence links.
   Add skill guidance only when a reusable procedure is needed; do not copy policy.
   Report implementation gaps and migrations separately from passing unit assertions.

Stop at a reviewable scoped change. Commit, deployment and external mutations require
their own applicable authorization; this workflow supplies none.
