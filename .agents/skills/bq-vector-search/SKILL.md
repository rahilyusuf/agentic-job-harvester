---
name: bq-vector-search
description: Implement or review BigQuery candidate retrieval, embedding storage/query contracts, cosine ranking, or vector-index evaluation. Use for retrieval and repository changes; model-call routing belongs to the gateway skill.
---

# BigQuery retrieval and vectors

## Policy and evidence

Read [guardrails](../../rules/guardrails.md) G-4/G-5/G-8 and
[architecture](../../../ARCHITECTURE.md#implemented-tier-1-python-orchestration).
They own isolation, candidate budgets, keys and write safety. This skill owns query
and retrieval procedures, not another model/provider or state policy.

**Implemented:** [VectorMatcher](../../../retrieval/vector_matcher.py) loads the user's
active centroid and calls `IEmbeddingRepository.vector_search()`. The
[concrete adapter](../../../repositories/embeddings_repo.py) orders
`1 - ML.DISTANCE(embedding, query_vector, 'COSINE')` and filters raw jobs by readiness.
It does not invoke `VECTOR_SEARCH`. [Index DDL](../../../sql/job_embeddings.sql) is
intent; live index readiness, query index usage and latency are **Unverified**.

## Working procedure

- Read repository protocols and actual SQL before selecting an optimization. Preserve
  metric semantics, ordering, job/score association and compatible vector dimensions.
  Delegate embedding generation/routing to [litellm-instructor](../litellm-instructor/SKILL.md).
- Use interface injection for business callers and parameterized values in adapters.
  Validate dimensions and finite values at boundaries; a raw list type alone does not
  establish compatibility. Schema versus SQL disagreements need explicit resolution.
- Retain the precomputed job vectors and the G-5 retrieval/prefilter budget. The current
  prefilter caps passed results, but YOE checks require optional extraction data absent
  from the current runner. Do not claim regex description parsing already exists or add
  raw-stream LLM extraction to make that claim true.
- Keep global ingestion readiness distinct from per-user evaluation decisions when
  designing a migration; current global status coupling is I-3 in the
  [backlog](../../rules/remediation-backlog.md), not a safe template.
- Reflection blends base resume and accepted-job centroid vectors. Read
  [reflection_agent.py](../../../agents/reflection_agent.py) for the gradual alpha
  schedule; its ceiling is not the initial default weight. Never mix model/task spaces
  just because vectors have equal length.

## Validation and planned optimization

Use mocked adapters for empty/missing profiles, malformed vectors, exact ranking,
limits, filtering and job/score identity. Existing tests live in
[tests/retrieval](../../../tests/retrieval). An indexed-search proposal needs authorized
representative benchmarks for recall, latency, cost and index availability (A-3).
Do not run DDL, backfills or cloud benchmarks merely to validate documentation, and
do not advertise the historical latency/cost estimates as measurements.
