# Architecture — implementation baseline and planned design

Evidence reviewed from repository source on 2026-11-14. This replaces the historical
LLD v3.1 presentation of intended behavior as completed architecture. No runtime,
cloud, package-installation, or deployment verification was performed for this review.

## Reading this document

- **Implemented:** executable source exists; linked tests indicate coverage, not a
  claim that tests were run during documentation alignment or that production works.
- **Planned:** intended behavior or a requirement not fully implemented. This label
  does not approve a migration or authorize implementation.
- **Unverified:** requires runtime, compatibility, deployment, or measurement evidence.

[AGENTS.md](AGENTS.md) owns precedence. [Guardrails](.agents/rules/guardrails.md) own
security/privacy/cost requirements; [conventions](.agents/rules/conventions.md) own
layer boundaries. The [remediation backlog](.agents/rules/remediation-backlog.md)
records unresolved findings separately. Their existence does not waive requirements.

## Implemented ingestion and embedding flow

```text
Apify ACTOR.RUN.SUCCEEDED event
  -> receiver authenticates and verifies actor run/dataset
  -> Dataset API fetch -> career-scraper normalization/sanitization
  -> BigQuery raw_job_postings
  -> Pub/Sub job-embedding-queue
  -> embedding worker -> gateway embedding helper -> Vertex prediction
  -> BigQuery job_embeddings; raw status EMBEDDING_DONE
```

**Implemented:** [services/apify_receiver.py](services/apify_receiver.py) receives a
small event, not a batch of job postings. It requires webhook secret configuration
in production and accepts configured HMAC/shared-secret/Bearer credentials. It verifies
run success, dataset provenance, and actor allowlists when configured. Apify API tokens
are sent in Authorization headers. Do not infer native Apify HMAC signing from the
receiver's optional HMAC support; the producer authentication setup is **Unverified**.

[services/normalizers/apify_career_scraper.py](services/normalizers/apify_career_scraper.py)
normalizes fields, handles apply-URL fallback, and sanitizes fields without prompt
wrappers before constructing `RawJob`. IDs use ATS identity where available or URL
hash fallback; [utils/ids.py](utils/ids.py) owns identity helpers.

[repositories/raw_jobs_repo.py](repositories/raw_jobs_repo.py) checks all retained raw
IDs, inserts novel records as `NEW_RAW`, and returns inserted IDs. Ninety days is a
freshness/discovery window, not an identity reset. Legacy streaming remains vulnerable
to concurrent inserts and cannot retain identity beyond raw-row expiration.
The receiver publishes those IDs, plus existing `NEW_RAW` IDs recovered on webhook
retry. `/reconcile` and `/jobs/reconcile` rediscover saved/unembedded jobs independently.
Publication futures are resolved in an executor and failures are counted. This is
recovery around separate persistence/publication operations, not a transactional outbox
or an exactly-once guarantee. See S-1 and I-2 for unresolved protection/concurrency gaps.

[services/embedding_worker.py](services/embedding_worker.py) decodes Pub/Sub pushes,
loads a job, processes `NEW_RAW`/`EMBEDDING_QUEUED`, writes the embedding, and marks
`EMBEDDING_DONE`. Caught failures reset to `NEW_RAW` and propagate for retry; hard
process termination and concurrent deliveries require additional recovery (I-2).

Evidence tests: [test_apify_receiver.py](tests/services/test_apify_receiver.py),
[test_apify_career_scraper.py](tests/services/test_apify_career_scraper.py), and
[test_embedding_worker.py](tests/services/test_embedding_worker.py). Preserve ingestion
behavior, credentials/provenance checks, publication recovery, and these regressions.
Generic n8n ingestion is **Planned**; this actor-specific receiver is not evidence for it.

### Opt-in durable publication and worker claims

The flow above describes the default legacy path. **Implemented, opt-in:**
`PUBLICATION_LEDGER_ENABLED` selects atomic raw-job/publication-intent DML in
`repositories/raw_jobs_repo.py`, the DML-only `repositories/publications_repo.py`,
and the shared dispatcher/composition in `services/publication_dispatcher.py` and
`services/ingestion_publications.py`. `sql/job_publications.sql` defines the sidecar;
raw schemas, source IDs, provider settings and webhook request shape stay unchanged.
`BatchInsertResult` separates inserted/existing/rejected/unconfirmed IDs; publication
and processing claims are typed in `schemas/publication_models.py`.

PENDING/IN_FLIGHT/PUBLISHED/UNKNOWN describe broker publication, not embedding readiness.
Transactions serialize mutations through a no-op DELETE guard; leased claims and fenced
checkpoints suppress routine republication. Lifetime identity also consults the retained
ledger across topics/versions; recrawls preserve original content and status. Missing raw
rows behind a retained identity and duplicate raw IDs fail closed for review.

The dispatcher checks ownership and raw-row uniqueness before each group, checks its
monotonic lease budget before SDK submission, and checkpoints at most ten attempts.
Valid outcomes commit independently of stale claims. SDK uncertainty is resolved through
the same query job ID; only proven transaction aborts permit a bounded fresh transaction.
Checkpoint retries do not resend messages. Publication counts require durable checkpoints.
The dispatcher has a 900-second bound and never claims exactly-once publication:
broker-acceptance/checkpoint crashes and final-check/send races remain at-least-once.
The worker's opt-in claim commits vector persistence, raw status and processing completion
together. Busy deliveries retry instead of ACKing incomplete work; completed duplicates
skip. This is not a guarantee of exactly-once provider computation after crash/timeout.

**Unverified / rollout required:** live schema, SQL execution/transaction contention,
streaming-buffer clearance, IAM, routing, schedules and migration evidence. The feature
defaults off; no application startup creates resources or migrates existing jobs.
`scripts/plan_publication_migration.py` only produces a local manifest: existing NEW_RAW
jobs without evidence remain UNKNOWN, with no automatic replay or fabricated timestamps.
See [rollout and contracts](docs/ingestion-publications.md) and mocked
[reliability tests](tests/services/test_publication_reliability.py). HTTP reconciliation
now requires an explicitly configured `RECONCILIATION_SECRET`, independently of the flag.

**Implemented tooling, live behavior Unverified:**
`scripts/verify_publication_ledger.py` prepares an offline resource/source manifest and
separately gates isolated provisioning, metadata checks, real SDK scenarios and cleanup.
It reuses normalization, persistence, ingestion composition and the default dispatcher;
it neither invokes Apify nor consumes subscriptions nor activates production flags.
Execution requires scoped IAM review and Python 3.11. The integration tool, local
environments, credentials and fixtures are excluded from the production Docker context.

## Model access

**Implemented:** [agents/base.py](agents/base.py) defines an abstract Python
`BaseIntelligenceAgent`, not an ADK `BaseAgent`. `_call_llm()` delegates structured
output to `GatewayClient.create()` in [gateway/client.py](gateway/client.py), which
wraps LiteLLM with Instructor. Provider mappings and chat aliases are configured in
[gateway/litellm_config.yaml](gateway/litellm_config.yaml); the maintained mapping
snapshot is in [stack.md](.agents/rules/stack.md#model-configuration).

**Unverified:** actual chat traffic traverses the intended proxy, fallback/rate/budget
settings are effective, and SDK callback APIs match the deployed dependency versions.
Setting LiteLLM globals is not sufficient evidence. There is no demonstrated dedicated
10-RPM Pro cap. Cost figures and historic latency estimates are unmeasured targets.

**Implemented deviation from G-1:** `generate_embedding(text: str) -> list[float]`
initializes AI Platform and offloads `_blocking_embed()` to an executor. The latter
calls Vertex `PredictionServiceClient` directly for `text-embedding-004`, checks 768
values, and hardcodes `us-central1` despite a configurable region. It does not call
LiteLLM `/embeddings`; no embedding alias exists in proxy configuration.

**Planned requirement, decision outstanding (A-1):** proxy-controlled embeddings remain
the target. This documentation neither approves the direct adapter permanently nor
authorizes changing it. A future routing decision must preserve provider/model identity,
vector compatibility, dimensions, input formatting, privacy, region behavior, retries,
and backfill compatibility; switching providers is a separate decision.

The worker builds `f"{title} {company} {location or ''} {description[:2000]}"`.
[services/user_profile_service.py](services/user_profile_service.py) calls the same
embedding helper with profile-specific title/skills/resume text. These are distinct
input contracts, not one universal job formatting rule. Resume material remains
subject to G-2 even when passed to the embedding helper.

## Implemented Tier 1 Python orchestration

[services/tier1_matcher_runner.py](services/tier1_matcher_runner.py) constructs adapters,
loads active users and metadata-derived resume summaries, and runs retrieval/scoring.
Daily Cloud Scheduler invocation is **Planned/Unverified**, not established by a Python
entry point alone.

1. [VectorMatcher.retrieve](retrieval/vector_matcher.py) reads `UserProfile.e_active_user`
   through interfaces and calls `IEmbeddingRepository.vector_search()`, default top 50.
2. [repositories/embeddings_repo.py](repositories/embeddings_repo.py) ranks
   `1 - ML.DISTANCE(e.embedding, query_embedding, 'COSINE')`, joining jobs whose raw
   status is `EMBEDDING_DONE`. This is an exact distance query, not a `VECTOR_SEARCH`
   invocation. [sql/job_embeddings.sql](sql/job_embeddings.sql) declares an IVF index;
   deployed index availability/usage and the old `<15ms` target are **Unverified**.
3. [RegexPrefilter.run](retrieval/regex_prefilter.py) rejects excluded title patterns
   and caps results (default 15). Its YOE/management extraction checks apply only when
   optional `JobStructuredExtraction` data is supplied. The current runner supplies
   none; a description-wide YOE gate is therefore **Planned**, not implemented.
4. [Tier1Pipeline.run](orchestration/pipeline.py) calls `JobIntakeRouter.route()`.
   Router SKIP returns before scorer/skeptic calls and updates raw status to `SKIPPED`.
5. Scorer then Skeptic execute once; a second explicit pair runs when
   `request_round_2` is true and no veto. There is no third-round path. The Skeptic
   computes delta against the settings threshold (default 10) and forces a request
   when exceeded, but does not clear every model-requested below-threshold request.
6. An `EvaluatedJob` is built using the Skeptic's agreed score and veto flag, persisted,
   and raw status becomes `SCORED`. Veto suppresses round two but currently still yields
   an evaluation rather than the hard-skip result promised by the older design (I-4).

Evidence: [test_pipeline.py](tests/orchestration/test_pipeline.py),
[test_scorer_debate.py](tests/agents/test_scorer_debate.py),
[test_router_agent.py](tests/agents/test_router_agent.py), and retrieval tests.
Repository method names do not establish idempotency; see I-1. Global raw-status changes
also exclude a job from subsequent users' retrieval (I-3).

## Tier 2 and research

**Required/Planned enforcement:** G-5 defines consent, ownership, eligibility, and veto
checks. It uses the actual `agreed_score` field; there is no `match_score` field.

**Implemented:** [HITLController](orchestration/hitl_controller.py) records actions and
publishes Analyze messages to `tier2-analysis-queue`. [Tier2Pipeline](orchestration/pipeline.py)
updates status, runs company research, then gathers verification, red-flag detection,
and skill-gap analysis concurrently using `asyncio.gather`. Results are validated and
returned. Status writes are unconditional; they do not acquire an optimistic lock.
There is no established dedicated Tier 2 consumer/result persistence path in this tree.
Do not claim end-to-end Tier 2 deployment or durable diligence results.

[CompanyResearchAgent](agents/research_agent.py) performs a predetermined cache/search/
history/page-fetch sequence followed by one structured synthesis. `_MAX_ITERATIONS=5`
bounds its counted operations, but cache lookup/write accounting is not an exact total
cap. It is not a model-directed ADK ReAct loop. [google_search](tools/search_tools.py)
uses Google Custom Search HTTP APIs, not an ADK-native search tool. Public/company
research, user application history, and cache isolation need separation (S-6).

[SkillGapAgent](agents/skillgap_agent.py) uses a validated local intermediate model
and generates challenges for blocking gaps via [execute_in_sandbox](tools/e2b_tools.py).
`CodeChallenge` records generated code, bounded output, success, elapsed time and sandbox
ID. The tool uses `AsyncSandbox`; deployed SDK compatibility and actual deadline/cleanup
semantics are **Unverified**, and settings allow longer than G-6's limit (S-10).

## Function and data contracts

**Implemented:** agents exchange explicit parameters and returned Pydantic models.
There is no implemented `ctx.session.state`/`output_key` registry. Do not invent one
from historic documentation. Method names below refer to their defining modules;
read source for full signatures/defaults rather than copying them into new code.

| Entry point | Current arguments | Return contract / evidence |
|---|---|---|
| `JobIntakeRouter.route` | `job: RawJob`, `user_id: str` | `RouterDecision`; [routing_models.py](schemas/routing_models.py) |
| `ScorerAgent.score` | job, user ID, `resume_summary: str`, `preference_rules: list[dict]`, round, optional feedback/trace | `ScorerOutput`; [scorer_agent.py](agents/scorer_agent.py), [scoring_models.py](schemas/scoring_models.py) |
| `SkepticAgent.audit` | job, user ID, `scorer_output: ScorerOutput`, round, optional trace | `SkepticOutput`; [skeptic_agent.py](agents/skeptic_agent.py) |
| `CompanyResearchAgent.research` | job, user ID | `CompanyResearch`; [research_models.py](schemas/research_models.py) |
| `CompanyVerificationAgent.verify` | job, `research_text: str`, optional trace | `CompanyProfile`; [company_agent.py](agents/company_agent.py) |
| `RedFlagDetectorAgent.detect` | job, research text, optional trace | `RedFlagAnalysis`; [redflag_agent.py](agents/redflag_agent.py) |
| `SkillGapAgent.analyze` | job, user ID, resume summary, optional trace | `SkillGapReport`; [diligence_models.py](schemas/diligence_models.py) |
| `ReflectionAgent.reflect` | user ID | `PreferenceRules`; [reflection_models.py](schemas/reflection_models.py) |
| `Tier1Pipeline.run` | job, user ID, resume summary, optional `list[dict]` rules | `Tier1PipelineResult` dataclass containing optional `EvaluatedJob` |
| `Tier2Pipeline.run` | job, user ID, resume summary | `Tier2PipelineResult` dataclass containing typed diligence results |

[OutputValidator.validate](orchestration/output_validator.py) revalidates model dumps
and returns the original model. Dataclasses, HTTP/Pub/Sub envelopes and validated storage
serialization are separate from LLM output schemas. Some tools return dictionaries;
that is a contract gap, not permission for unvalidated LLM outputs.

`PreferenceRules` is the aggregate model with `rules: list[PreferenceRule]`. Router and
reflection loaders currently return rule dictionaries; the scorer accepts `list[dict]`.
The Tier 1 runner does not pass the router's loaded rules into scoring (I-5).
`RawJob` and `EvaluatedJob` are defined in [job_models.py](schemas/job_models.py).
`UserProfile` contains resume metadata/pointer and vectors; the profile service receives
resume text via request input, not an implemented Secret Manager resume-fetch path.

[repositories/interfaces.py](repositories/interfaces.py) defines protocols. Its raw
batch insertion return type is `int`, while the concrete receiver adapter returns
`list[str]`; recovery methods are also missing from that protocol (I-5). Do not claim
protocol conformance is complete.

## State and persistence ownership

| Contract | Implemented values / owner | Requirement or unresolved behavior |
|---|---|---|
| Global raw lifecycle | `JobStatus` declares `NEW_RAW`, `EMBEDDING_QUEUED`, `EMBEDDING_DONE`, `SCORED`, `TIER2_PENDING`, `COMPLETE`, `SKIPPED` | Enum membership does not enforce legal transitions. |
| Ingestion | Receiver/repository insert `NEW_RAW`; receiver/reconciliation publish IDs | Publishing does not itself mark `EMBEDDING_QUEUED`. |
| Embedding | Worker writes `EMBEDDING_QUEUED` then `EMBEDDING_DONE`; caught failure resets `NEW_RAW` | Stale in-progress recovery remains I-2. |
| Per-user evaluation | Tier 1 writes `(job_id, user_id)` evaluation and also global `SCORED`/`SKIPPED` | Separate global readiness from per-user decisions in a future migration (I-3). |
| Tier 2 | Pipeline writes `IN_PROGRESS`, `COMPLETE`, `ERROR`; schema describes nullable/PENDING/IN_PROGRESS/COMPLETE | Free-form string; no atomic claim. Old `DONE`/`FAILED` prose is not the current contract. |
| Actions | `ActionsRepository` appends apply/pass/analyze with trace metadata | Replay idempotency/authorization unresolved. |
| Profile | Profile service writes metadata/pointers/vectors; reflection updates active centroid | Current profile upsert is append-based (I-1). |
| Trace | Pipeline generates trace metadata; callbacks carry a supplied trace ID | `EvaluatedJob`/DDL do not persist a scoring trace field; reliable delayed correlation is Planned (S-9). |

[sql/raw_job_postings.sql](sql/raw_job_postings.sql),
[sql/job_embeddings.sql](sql/job_embeddings.sql),
[sql/job_evaluated.sql](sql/job_evaluated.sql),
[sql/job_user_actions.sql](sql/job_user_actions.sql), and
[sql/user_profiles.sql](sql/user_profiles.sql) declare intended table shapes. Live
schemas/migrations are **Unverified**. Do not normalize statuses or add columns merely
while updating documentation; design compatibility/migrations first.

## Observability and reflection

**Implemented:** `observability/setup.py` preserves named sync/async agent decorators
using native Langfuse 4 span observations without argument/result or exception-content
capture. The gateway creates one child generation per logical Instructor invocation;
this is not a count of provider requests or verified token/cost accounting. Controlled
metadata is allowlisted and explicit identifiers are hashed. Telemetry entry/update/exit,
feedback and flush errors are best-effort; business failures and cancellation propagate.
Application-owned automatic Langfuse callbacks are removed without replacing unrelated
LiteLLM callbacks. The proxy YAML disables its automatic Langfuse callbacks; a separately
running proxy must be checked independently. No model/provider routing changes occur.

**Unverified:** live SDK/proxy export and deployed configuration, reliable end-to-end
trace linkage and feedback ownership. Current application UUID metadata does not establish
an SDK trace context. Flushing is not called by every service lifecycle. See S-5 and S-9;
this P0 correction does not close those broader findings.

**Implemented:** ingestion events retain stable names, request correlation and compact
INFO summaries. `observability/diagnostics.py` supplies relative emitter and application
failure locations, skipping SDK/retry infrastructure frames. Named dependency boundaries
distinguish BigQuery duplicate checking from streaming insertion. Only allowlisted status,
reason and explicit job-ID metadata is captured; exception messages, SQL and frame locals
are excluded. WARN/ERROR consoles show concise diagnostics; JSON/file output keeps the
structured metadata. Retry classification is diagnostic only: this does not change the
legacy retry policy or establish the cause/outcome of a historical BigQuery failure.
The receiver also emits its effective BigQuery location and ledger flag at startup,
before any webhook, without constructing a repository or exposing other settings.

[ReflectionAgent](agents/reflection_agent.py) reads 30-day feedback and accepted
embeddings, computes `(1 - alpha) * e_resume + alpha * accepted_centroid`, and gradually
increases alpha toward its 0.35 ceiling. It synthesizes preferences and writes Firestore.
It currently supplies accepted job IDs and no passed-job summaries to synthesis;
joining full job evidence and Langfuse traces is **Planned**. Weekly execution and
end-to-end preference quality are **Unverified**. Analysis requests currently receive
0.5 acceptance scores, conflicting with G-9's target semantics.

## Planned architecture and outstanding decisions

- **Planned, not approved for implementation here:** Google ADK integration. Packages
  and comments do not establish use of `SequentialAgent`, `LoopAgent`, `ParallelAgent`,
  or `LlmAgent`. Retain current Python orchestration unless a separate migration is approved.
- **Planned:** authenticated, atomically claimed Tier 2 execution and durable results;
  typed preference/tool contracts and complete reflection evidence.
- **Planned:** evaluate native indexed retrieval against exact ranking with measured
  recall/cost/latency; do not assume an index is used by `ML.DISTANCE`.
- **Unverified:** cloud IAM, Pub/Sub push identity, schedules, live indexes/schemas,
  deployment health, budget/rate enforcement, and SDK versions.

Decision details, exact affected files, dependencies, regression risks, and security
work are maintained in the [remediation backlog](.agents/rules/remediation-backlog.md).
Changes to boundaries, gates, model routing, state or persistence contracts require
synchronized architecture documentation and explicit implementation scope.
