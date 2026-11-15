# Agentic Job Harvester

Job intelligence for an early-career AI/ML engineer targeting Saudi Arabia and the UAE.
Current source uses async Python orchestration, BigQuery, Pub/Sub, Instructor/LiteLLM,
Langfuse and E2B. Google ADK integration is **Planned**, not the implemented runtime.

## Architecture status

- **Implemented source:** authenticated/provenance-checked Apify event ingestion,
  actor-specific normalization/sanitization, BigQuery writes, Pub/Sub publication
  recovery/reconciliation, and an embedding worker.
- **Implemented source:** cosine ranking using `ML.DISTANCE`, a bounded deterministic
  prefilter, router/scorer/skeptic Python calls, and typed outputs. Declared IVF DDL
  does not establish index usage by the current query.
- **Partially implemented:** on-demand diligence with research followed by concurrent
  verification, red-flag and skill-gap operations, E2B challenge execution, Telegram
  actions, and reflection. Authorization, concurrency, persistence and privacy gaps
  are recorded separately; a complete Tier 2 worker deployment is not established.
- **Implemented routing deviation:** embeddings use direct Vertex prediction inside
  the gateway. Proxy-controlled embedding routing remains the target; a permanent
  routing decision has not been approved. Chat aliases come from gateway configuration.
- **Unverified:** deployed services/schedules/IAM, SDK compatibility, proxy enforcement,
  live schema/index readiness, and historical latency/cost estimates.

## Repository guidance

Start with [AGENTS.md](AGENTS.md) for instructions and policy ownership,
[ARCHITECTURE.md](ARCHITECTURE.md) for evidence and current contracts, and
[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for actual modules/entry points.
[Security requirements](.agents/rules/guardrails.md) remain binding even where
[the remediation backlog](.agents/rules/remediation-backlog.md) records missing enforcement.

## Local setup guidance

[requirements.txt](requirements.txt) and [pyproject.toml](pyproject.toml) declare the
runtime/development baseline; broad ranges are not a tested environment lockfile.
[Stack guidance](.agents/rules/stack.md) explains configuration evidence and limitations.
Do not use the former `adk web agents/` instructions for this Python implementation.

The local receiver entry point is `services.apify_receiver:app` (Uvicorn/FastAPI).
Starting it can resolve secrets and contacting its ingestion endpoint can write to
cloud services. Use an explicitly configured development environment for application
work; a server launch, bootstrap script or Cloud Build run is not a documentation check.
