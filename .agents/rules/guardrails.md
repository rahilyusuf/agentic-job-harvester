# Security & Guardrails — agentic-job-harvester

## Hard Rules (never violate)

### G-1: Zero Direct LLM Calls
Every Gemini Flash/Pro/Embedding call MUST route through `gateway/client.py`.
- ❌ `genai.Client().models.generate_content(...)` directly in agent code
- ❌ `vertexai.generative_models.GenerativeModel(...)` anywhere outside gateway/
- ✅ `from gateway.client import get_instructor_client, generate_embedding`

### G-2: Secrets via Secret Manager Only
- Never hardcode API keys, tokens, or credentials in source files
- Never commit `.env` with real values (`.env` is in `.gitignore`)
- All secrets fetched via `config/settings.py` which reads Secret Manager in production
- Local dev uses `.env` file with dummy/test values

### G-3: Schema Enforcement Everywhere
- All LLM outputs must be validated against a Pydantic v2 schema in `schemas/`
- Use `instructor` client — never parse raw LLM JSON string manually
- Unvalidated `dict` returns from any agent are a hard bug

### G-4: Repository Interface Isolation
- Agents and orchestration code MUST import from `repositories/interfaces.py` (Protocol ABCs)
- Never import concrete `*_repo.py` classes directly in `agents/`, `orchestration/`, or `services/`
- Dependency injection via constructor: `def __init__(self, repo: IRawJobRepository)`

### G-5: Cost Discipline
- Never call LLM scoring over raw unindexed job streams
- Tier 1 scoring activates ONLY after `retrieval/` yields ≤15 candidates
- Tier 2 (diligence swarm) activates ONLY on explicit user `[🔍 Analyze]` action
- `JobIntakeRouter` SKIP path must short-circuit before any other agent runs

### G-6: E2B Sandbox Isolation
- All generated Python code executes in `e2b.AsyncSandbox` — never `exec()` locally
- Sandbox timeout: 30 seconds (configured in `config/settings.py`)
- Sandbox results must be length-capped before storing in `schemas/diligence_models.CodeChallenge`

### G-7: HITL State Machine
- Telegram HITL pause/resume state is managed ONLY in `orchestration/hitl_controller.py`
- Never block a Cloud Run request thread waiting for HITL — use async Pub/Sub correlation
- User actions (apply/pass/analyze) emit events to `job_user_actions` via `repositories/actions_repo.py`

### G-8: BigQuery Write Safety
- All BQ inserts must include `job_id` deduplication (90-day window check via `utils/time.py`)
- `raw_job_postings.status` tracks the overall job lifecycle through the pipeline:
  `NEW_RAW → EMBEDDING_QUEUED → EMBEDDING_DONE → SCORED → COMPLETE`
  Never update `status` outside the designated service (e.g., only `embedding_worker` sets `EMBEDDING_DONE`).
- `job_evaluated.tier2_status` is a **separate, independent field** used solely as an
  optimistic lock for the on-demand Tier 2 swarm: `PENDING | IN_PROGRESS | DONE | FAILED`.
  It guards against double-firing `DueDiligenceSuite` when a user clicks `[🔍 Analyze]`
  multiple times or across devices. Do not conflate it with the job lifecycle `status`.

### G-9: LangFuse Correlation
- Every agent invocation must carry a `trace_id` from `observability/setup.py`
- LiteLLM must be configured with LangFuse as a callback (set in `observability/setup.py`)
- `langfuse.score()` must be called on every user action with `user_acceptance` metric

### G-10: No Sideways Dependencies
- Dependency flow: `services → orchestration/agents → retrieval/tools → repositories/interfaces`
- `schemas/` is depended on by everything; it imports nothing from other project modules
- `gateway/` is depended on by **`agents/base.py`** (structured LLM calls),
  **`services/embedding_worker.py`** (embedding generation), and
  **`services/tier1_matcher_runner.py`** (if it triggers any LLM routing).
  Never imported in `schemas/` or `repositories/`.
