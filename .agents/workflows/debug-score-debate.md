# Debug the Python score debate

Use for scoring/debate regressions, not deployment or a framework migration. Read
[the pipeline skill](../skills/pipeline-orchestration/SKILL.md),
[guardrails](../rules/guardrails.md) G-3/G-5/G-9, and
[architecture](../../ARCHITECTURE.md#implemented-tier-1-python-orchestration).

## Trace the actual control flow

1. Inspect `Tier1Pipeline.run()` in [pipeline.py](../../orchestration/pipeline.py),
   [ScorerAgent.score](../../agents/scorer_agent.py),
   [SkepticAgent.audit](../../agents/skeptic_agent.py),
   [scoring models](../../schemas/scoring_models.py) and the configured delta threshold
   in [settings.py](../../config/settings.py). Read files; do not import settings to
   inspect a default because initialization may access secrets.
2. Reproduce with synthetic models and mocked clients. Confirm routing SKIP invokes
   neither scorer nor skeptic. For processed jobs, capture call order, rounds, feedback,
   validated outputs and repository writes without capturing resume text/summaries.
3. Compare delta below/equal/above the threshold, model-requested round two, and veto.
   Current Skeptic logic forces some requests but does not clear every below-threshold
   request; current veto suppresses round two yet still persists a scored evaluation.
   These are I-4 findings, not guaranteed target behavior. A diagnosis should distinguish
   a regression from an existing documented gap.
4. Check the second scorer receives the prior Skeptic rationale and that the final
   Skeptic result determines the evaluation. Count actual invocations and confirm no
   path adds a third round. Schema validation alone does not enforce all business rules.
5. Check model revalidation, persistence failures and raw status effects. An evaluation
   repository named upsert is not evidence that the write was unique/successful (I-1).
   A score/notification alone must not trigger Tier 2 under G-5.

## Verification and reporting

Preserve [existing debate tests](../../tests/agents/test_scorer_debate.py) and
[pipeline tests](../../tests/orchestration/test_pipeline.py); add focused regressions
for authorized fixes. Use [tracing guidance](../skills/langfuse-tracing/SKILL.md) when
investigating correlation, never production prompt dumps. Do not call live providers,
change model aliases, deploy services, or introduce ADK to reproduce a local branch.

Report observed inputs/control flow, the canonical requirement, the minimal proposed
fix, validation and any remaining [backlog](../rules/remediation-backlog.md) dependencies.
