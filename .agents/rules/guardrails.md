# Security, privacy, cost, and data guardrails

This file is the canonical requirement owner under [AGENTS.md](../../AGENTS.md).
Requirements remain binding even when current code does not enforce them.
[remediation-backlog.md](remediation-backlog.md) records gaps without granting waivers.

## G-1 — Centralized, proxy-controlled model access

All model access must enter through `gateway/client.py`. Structured chat uses its
Instructor wrapper and configured LiteLLM proxy aliases; agents must not call provider
SDKs directly or parse raw model JSON as an alternative scoring path. The target
remains proxy routing for embeddings as well, using the proxy embedding endpoint,
not Instructor's chat API. The current gateway's direct Vertex prediction call is an
unresolved implementation deviation (A-1), not a permanent approved exception. Preserve
it during documentation work; do not copy it elsewhere or change providers to satisfy
outdated prose. Verify actual proxy routing rather than assuming client globals prove it.

## G-2 — Secrets and candidate privacy

Never hardcode/commit credentials or real `.env` values. Production secrets must be
resolved centrally through `config/settings.py` and Secret Manager; local fixtures use
dummy values. Secret resolution failure must not silently authorize protected work.
Never log raw resume text, include it in Langfuse inputs/outputs, or write it into
BigQuery rows. Apply equivalent capture restrictions to resume-derived summaries,
embedding request bodies, exception details, and tokens in URLs. Resume content is
in-memory input; document actual source handling rather than claiming it is always
loaded from Secret Manager. Non-sensitive metadata, hashes, and authorized pointers
are separate contracts, not permission to persist resume text.

## G-3 — Validated contracts

Every LLM output must validate against a Pydantic v2 schema. Shared contracts belong
in `schemas/`; local intermediate models must also be validated. Revalidate
post-construction changes before downstream use/persistence; do not bypass validation
with raw dictionaries. Serialized dictionaries at storage/transport boundaries must
derive from validated contracts. Existing untyped tool results/protocol mismatches are
backlog, not a reason to invent ADK state. Validate identity/provenance fields rather
than trusting model-produced IDs to select a different user/job.

## G-4 — Repository isolation

Agents, orchestration, and retrieval depend on `repositories/interfaces.py`, not
concrete `*_repo.py` classes. Services may construct concrete adapters for dependency
injection as defined in [conventions.md](conventions.md#layer-responsibilities).
Keep SQL in adapters; database-backed tools should use interfaces.

## G-5 — Cost and bounded execution

Maintain precomputed job embeddings plus per-user scoring of retrieved candidates:
O(jobs) embedding work and O(users × top_k) scoring. Never score raw unindexed streams.
The baseline is top 50 retrieval and at most 15 candidates after the deterministic
prefilter; increasing this cost envelope requires an explicit architecture decision.
Router SKIP short-circuits before scoring. Debate has at most two rounds, with the
Skeptic's final score and veto authority; any future ADK LoopAgent must explicitly use
`max_iterations=2`. Research is bounded to five tool operations; accounting and early
exit must be explicit, not delegated to model compliance.

Tier 2 requires an authenticated explicit user Analyze action and an owned Tier 1
evaluation with `agreed_score >= 75` and no veto. A score threshold, schedule, or
notification alone never authorizes Tier 2. Notification baseline 85 is distinct.
Preserve this requirement despite missing enforcement (S-7).

## G-6 — Sandbox isolation

Execute generated challenge code only inside `e2b_code_interpreter.AsyncSandbox`,
never with local `exec`, `eval`, or subprocesses. Enforce a maximum 30-second execution
deadline and cleanup on success, error, cancellation, and timeout. Cap persisted stdout
at 2000 characters and stderr at 500. Record sandbox ID, duration, and execution result
in `CodeChallenge`. Current settings permit longer timeouts; see S-10. There is no
`SANDBOX_TIMEOUT_SECONDS` constant to import in the current implementation.

## G-7 — Authenticated, durable HITL

`orchestration/hitl_controller.py` owns action coordination; transport authentication
belongs to service/adapters. Never block a Cloud Run request waiting for human input.
Authenticate webhook/operator ingress, authorize user/job ownership, and handle replay
idempotently before recording actions or triggering expensive work. Record apply/pass/
analyze events in BigQuery. Callback payloads are untrusted; resolve authoritative IDs
and traces from durable state. Pub/Sub consumers require verified caller configuration.

## G-8 — Durable data and safe writes

Job records, embeddings, evaluations, and actions live in BigQuery. Firestore is only
for configuration and research caches; caches must not leak user-specific history.
Deduplicate raw ingestion by lifetime logical job ID; ninety days is a freshness
window, never an identity reset. Recrawls preserve original content/status. Strict
concurrent-insert and post-retention identity protection requires all writers to use
the opt-in durable ledger; legacy streaming remains best-effort. Use domain-appropriate
keys for other records: job ID for embeddings, `(job_id, user_id)` for evaluations,
user ID for profiles, event identity for actions. A universal job-ID dedupe requirement
cannot apply to profiles or legitimately repeated action types.

Parameterize SQL values and validate configured SQL identifiers. Do not report success
after write errors or call append-only writes idempotent upserts. Account for partial
writes, retries, and concurrent deliveries. Update lifecycle states only through their
designated owner in the architecture; conditional claims must check affected rows.
Preserve publication recovery/reconciliation and never publish unconfirmed failed writes.

## G-9 — Safe observability and correlation

Trace every model invocation with privacy-safe instrumentation and reliable job/user
correlation. Current public agent methods use `@observe`; do not require a nonexistent
`_invoke_llm` method. Define one intentional client/proxy callback strategy, verify the
supported SDK API, and flush at shutdown. Automatic prompt capture cannot override G-2.
Persist the actual scoring trace ID with the evaluation or trusted durable message
metadata so delayed actions score the correct trace. Record every action; acceptance
feedback is apply=1 and pass=0. Analyze is an information request and must not be
an acceptance label (current 0.5 behavior is S-9).

## G-10 — External content trust boundaries

All scraped/webhook/page/search/cache-derived text must pass through the shared
`orchestration/input_sanitizer.py` before entering prompts. Preserve ingestion field
sanitization and also isolate content at the prompt boundary with escaped delimiters,
length caps, and a system instruction that external content is data, never instructions.
Do not rely on regex stripping or stored sanitized text alone. Do not route candidate
resumes through the scraped-content sanitizer; handle privacy under G-2. Restrict fetched
destinations/redirects, block internal and metadata targets, and bound download bytes/time.
Sanitization is defense in depth, not a guarantee against injection.

## G-11 — Architecture preservation

Follow the layer boundaries in [conventions.md](conventions.md). Do not modify pipeline
topology, invent alternative LLM scoring paths, add automatic Tier 2, introduce ADK,
or switch providers merely to match historical documentation. Such changes need
explicit task scope and synchronized architecture/contracts. Preserve the working
Apify → BigQuery → Pub/Sub → embedding flow during governance remediation.
