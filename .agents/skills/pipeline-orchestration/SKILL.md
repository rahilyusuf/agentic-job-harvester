---
name: pipeline-orchestration
description: Modify or debug the current Python Tier 1 debate and Tier 2 coordination, agent invocation contracts, or pipeline dependency injection. Use for orchestration changes, not as authorization to migrate to Google ADK.
---

# Python pipeline orchestration

## Ownership and activation

Use for sequencing, bounded debate, typed results, eligibility and concurrency in
`orchestration/pipeline.py`. For standalone retrieval, gateway or sandbox changes use
the corresponding skill from [AGENTS.md](../../../AGENTS.md#skills-and-workflows).
[Conventions](../../rules/conventions.md) own boundaries; [guardrails](../../rules/guardrails.md)
G-3/G-4/G-5/G-7 own validation, isolation, cost and consent. Do not redefine them here.

## Current implementation — Implemented

Read [pipeline.py](../../../orchestration/pipeline.py), [base.py](../../../agents/base.py)
and the [architecture contracts](../../../ARCHITECTURE.md#function-and-data-contracts)
before editing. Agents inherit a Python ABC; the pipeline uses direct async calls,
explicit conditional round two, and `asyncio.gather`. There is no ADK session registry.
Research is a predetermined tool sequence plus synthesis, not model-directed ReAct.

When modifying a sequence:

1. Follow actual signatures and Pydantic models. Keep orchestration dataclasses distinct
   from LLM response models. Pass optional trace metadata through supported parameters;
   do not assume every agent accepts it or that a UUID establishes SDK linkage.
2. Preserve Router short-circuiting, bounded debate, final Skeptic authority, and the
   separate Tier 1/Tier 2 invocation paths governed by G-5. Pass round-one feedback
   only through the existing scoring parameter or an explicitly updated contract.
3. Keep repository constructor types at protocol boundaries. Services construct concrete
   adapters; the pipeline currently constructs agents internally. Agent factory/client
   injection improvements are a separate implementation change, not an existing feature.
4. Revalidate outputs with `OutputValidator` after mutable overrides and before
   persistence. Preserve failure reporting; do not mark a write successful because a
   method named upsert returned after swallowing errors.

## Known gaps and planned work

[Backlog](../../rules/remediation-backlog.md) S-7, I-3/I-4/I-5 and A-2/A-4 cover the
missing Tier 2 gate/claim, global raw statuses, veto/consensus discrepancies, typed
preferences, and future ADK/worker decisions. An unconditional status update is not a
lock. Do not add scheduled Tier 2 or introduce framework classes to repair prose.

## Validation

For authorized implementation, use mocked agents and repository interfaces. Cover
SKIP making no scorer calls; delta boundaries; model-requested round-two behavior;
veto in either round; no third round; validation/write failures; and eligible versus
unauthorized/duplicate Analyze actions. Existing [pipeline tests](../../../tests/orchestration/test_pipeline.py)
and [debate tests](../../../tests/agents/test_scorer_debate.py) are evidence, not proof
that all missing controls already work. Use [debug-score-debate](../../workflows/debug-score-debate.md)
for a focused investigation. No live model/cloud calls are needed for unit validation.
