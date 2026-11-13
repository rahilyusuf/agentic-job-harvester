# ARCHITECTURE.md — LLD v3.1

## 1. Ingestion & Pre-Computation

**`apify-receiver` (Cloud Run)** — dumb ingestion consumer.
- Accepts webhook payloads from Apify actors / n8n.
- `job_id` = ATS-native UUID (Lever, Workable) or `MD5(job_url)` fallback.
- Dedupe against a 90-day BigQuery window before insert.
- Batch insert novel jobs into `raw_job_postings` with `status = 'NEW_RAW'`.
- Publish `job_id` to Pub/Sub topic `job-embedding-queue`.

**`job-embedding-worker` (Cloud Run)** — pre-computes embeddings once per job.
- Calls `text-embedding-004` on `f"{title} {company} {location} {description[:2000]}"`
  → 768-dim vector.
- Writes to `job_embeddings`, backed by a native BigQuery IVF Cosine vector index.

## 2. Tier 1 Retrieval & Agentic Scoring

**`tier1-vector-matcher`**
- **Step A — BQ Vector Search:** cosine similarity via
  `1 - ML.DISTANCE(e_active_user, e_job, 'COSINE')`, top 50 roles, <15ms.
- **Step B — Regex pre-filter ($0 cost):** drop YOE > 5yrs, exclude VP/Director/
  Manager titles. Yields ~15 roles.
- **Step C — LiteLLM routing:** all downstream agent calls go through the proxy;
  handles RPM limits, cost logging, auto-fallback `gemini-2.5-pro → gemini-2.5-flash`.
- **Step D — Agent 1: `JobIntakeRouter`** (custom `BaseAgent` + Instructor): reads
  Firestore `dynamic_preference_rules`, returns `SKIP | PROCESS | HIGH_PRIORITY`. On
  `SKIP`, halt immediately (~$0.0001 cost).
- **Step E — Agent 2: `ScoreDebateLoop`** (ADK `LoopAgent`, max 2 rounds):
  - `ScorerAgent` (flash) scores fit against resume + Firestore rules.
  - `SkepticAgent` (flash) audits for hidden disqualifiers.
  - Consensus rule: if `|initial_score - agreed_score| > 10`, run round 2.
    **Skeptic has final veto authority.**
  - Writes result to `job_evaluated`.

## 3. Tier 2 On-Demand Intelligence Swarm

Triggered only by user `[🔍 Analyze]` in Streamlit/Telegram. Never automatic.

- **Agent 3: `CompanyResearchAgent`** (ADK `LlmAgent`, ReAct, max 5 iterations) — 4
  tools: `lookup_research_cache` (Firestore), `google_search` (ADK native), `fetch_url`,
  `check_application_history` (BigQuery).
- **Agent 4: `DueDiligenceSuite`** (ADK `ParallelAgent`), three concurrent sub-agents:
  - **4a `CompanyVerificationAgent`** (flash) — headcount, entity type (MNC/VC
    startup/staffing agency), funding stage, recent news.
  - **4b `RedFlagDetectorAgent`** (flash) — ghost jobs (>60 days active), Glassdoor
    toxicity, unrealistic skill demands.
  - **4c `SkillGapAgent` + E2B sandbox** (flash) — blocking vs. learnable gaps; for
    blocking gaps, generates a Python practice challenge and executes it in an E2B
    `AsyncSandbox` micro-VM to verify syntax/output before returning to the user.
- **Output Validation Gateway** — Pydantic schema check gates everything before
  persistence.

## 4. Human-in-the-Loop & Active Learning Flywheel

- **Telemetry:** high-match roles (`score >= 85`) or low-confidence routing trigger a
  Telegram notification with inline actions (`✅ Apply`, `👎 Pass`, `🔍 Analyze`). User
  actions emit events to `job_user_actions`.
- **LangFuse:** LiteLLM auto-logs prompts/tokens/latency. On user action,
  `langfuse.score()` tags the trace with `user_acceptance` (1.0 applied / 0.0 passed).
- **Offline Reflection Agent** (`gemini-2.5-pro`, weekly Cloud Scheduler):
  - Queries 30 days of telemetry + LangFuse traces.
  - Synthesizes rules into Firestore `config_cache/dynamic_preference_rules`.
  - Recalculates the active vector centroid:
    `e_active_new = (1 - α) * e_resume + α * e_accepted_centroid`

## Technology alignment matrix

| Pattern | Tool | Component |
|---|---|---|
| Agent orchestration | Google ADK (`google-adk`) | SequentialAgent, LoopAgent (debate), ParallelAgent (diligence), ReAct LlmAgent |
| Durable state execution | Cloud Run + Pub/Sub + BQ | Optimistic locking via `tier2_status` |
| Code execution sandboxing | E2B (`e2b-code-interpreter`) | `SkillGapAgent` practice challenges |
| LLM gateway/routing | LiteLLM Proxy | Pro→Flash fallback, RPM buckets, cost tracking |
| Schema/output control | Instructor + Pydantic v2 | Auto-retry with validation error context |
| Observability | LangFuse | `@observe` tracing, LiteLLM callbacks, acceptance scoring |
| Vector search | BigQuery native vector index | `text-embedding-004`, `ML.DISTANCE` cosine |

## BigQuery tables (contract)
- `raw_job_postings` — raw ingested jobs. `status` column tracks the overall job lifecycle:
  `STRING, NOT NULL` — values: `NEW_RAW → EMBEDDING_QUEUED → EMBEDDING_DONE → SCORED → COMPLETE`.
  Only the designated service for each transition may write that value.
- `job_embeddings` — 768-dim vectors + IVF cosine index
- `job_evaluated` — Tier 1 scoring output. Contains two distinct status fields:
  - **`status`** (`STRING`): mirrors `raw_job_postings.status` at the point of scoring;
    written by `ScoreDebateLoop` when Tier 1 completes.
  - **`tier2_status`** (`STRING`): an independent optimistic-lock field used *only* by
    the on-demand Tier 2 swarm. Values: `PENDING | IN_PROGRESS | DONE | FAILED`.
    Purpose: prevents `DueDiligenceSuite` from double-firing when a user clicks
    `[🔍 Analyze]` multiple times or across devices. Written exclusively by
    `orchestration/hitl_controller.py`. Default on row creation: `PENDING`.
    This field is **not** part of the job lifecycle state machine — it is a narrow
    concurrency lock scoped to one on-demand operation. Do not conflate the two.
  - **`langfuse_trace_id`** (`STRING`): persisted here so HITL actions can attach
    scores to the correct trace after the ADK session has ended (see Session State
    Key Registry in AGENTS.md).
- `job_user_actions` — HITL telemetry (apply/pass/analyze events)
- `user_profiles` — candidate profile written by the User Profile Service (Streamlit
  → Secret Mgr → User Profile Service). Holds resume metadata pointer, `e_resume`
  base embedding, and current `e_active_user` centroid — this is what Step A’s
  `ML.DISTANCE(e_active_user, e_job, 'COSINE')` reads against, and what the weekly
  Reflection Agent updates via the centroid-drift formula.

## Change policy
Any change to agent boundaries, the Tier1/Tier2 gate, or table schemas must be
reflected here in the same change — this file is what keeps the agent from silently
reshaping the pipeline while coding.
