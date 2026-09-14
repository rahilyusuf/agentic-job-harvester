# PROJECT_STRUCTURE.md — Agentic Job Harvester

Corrected against `ARCHITECTURE.md` (LLD v3.1). Additions vs. the original draft are
marked `[NEW]`. This file is the folder-level source of truth — if you add/remove/
rename a top-level module, update this file in the same change.

```
agentic-job-harvester/
├── agents/                      ← ADK agent classes (one file per agent)
│   ├── base.py                  ← BaseIntelligenceAgent (Instructor + LiteLLM + LangFuse wrapper)
│   ├── router_agent.py          ← JobIntakeRouter (Dynamic Routing)
│   ├── scorer_agent.py          ← ScorerAgent (Tier 1 Debate)
│   ├── skeptic_agent.py         ← SkepticAgent (Tier 1 Debate & Veto)
│   ├── research_agent.py        ← CompanyResearchAgent (ReAct + Tools)
│   ├── company_agent.py         ← CompanyVerificationAgent (Due Diligence)
│   ├── redflag_agent.py         ← RedFlagDetectorAgent (Ghost/Toxicity Scan)
│   ├── skillgap_agent.py        ← SkillGapAgent (Gap Analysis + E2B Sandbox)
│   └── reflection_agent.py      ← ReflectionAgent (Weekly Active Learning)
│
├── retrieval/                   ← [NEW] Tier 1 candidate retrieval — belongs here,
│   │                               not in repositories/, because it's business logic
│   │                               (which roles qualify), not raw data access.
│   ├── vector_matcher.py        ← Step A: BQ VECTOR_SEARCH / ML.DISTANCE cosine, top 50
│   └── regex_prefilter.py       ← Step B: $0 gate — YOE>5yrs, excluded titles, top ~15
│
├── orchestration/                ← ADK graph wiring & non-agent logic
│   ├── pipeline.py               ← SequentialAgent, LoopAgent, and ParallelAgent graphs
│   ├── hitl_controller.py        ← Telegram pause/resume HITL state machine
│   └── output_validator.py       ← Pydantic Output Validation Gateway
│
├── schemas/                      ← Pydantic v2 data contracts (Single Source of Truth)
│   ├── job_models.py             ← RawJob, JobStructuredExtraction, EvaluatedJob
│   ├── routing_models.py         ← RouterDecision
│   ├── scoring_models.py         ← ScorerOutput, SkepticOutput
│   ├── research_models.py        ← CompanyResearch, CompanyProfile
│   ├── diligence_models.py       ← RedFlagAnalysis, SkillGapReport, CodeChallenge
│   ├── reflection_models.py      ← PreferenceRules
│   └── profile_models.py         ← [NEW] UserProfile, ResumeMetadata (for user_profiles)
│
├── tools/                        ← Tool definitions for ReAct and Sandboxing
│   ├── search_tools.py           ← google_search, fetch_url
│   ├── cache_tools.py            ← lookup_research_cache, write_research_cache
│   ├── history_tools.py          ← check_application_history
│   └── e2b_tools.py              ← execute_in_sandbox
│
├── repositories/                 ← BigQuery data access layer
│   ├── interfaces.py             ← [NEW] Protocols/ABCs — agents & orchestration depend
│   │                                on THESE, never on concrete classes below. This is
│   │                                the actual dependency-inversion mechanism, and what
│   │                                lets tests/ mock repos without touching real BQ.
│   ├── raw_jobs_repo.py          ← [NEW] raw_job_postings read/write
│   ├── embeddings_repo.py        ← [NEW] job_embeddings read/write
│   ├── evaluated_repo.py         ← [NEW] job_evaluated read/write
│   ├── actions_repo.py           ← [NEW] job_user_actions read/write
│   └── profiles_repo.py          ← [NEW] user_profiles read/write
│
├── sql/                          ← [NEW] BigQuery DDL — executable source of truth
│   │                                for table shape, not just ARCHITECTURE.md prose.
│   ├── raw_job_postings.sql
│   ├── job_embeddings.sql
│   ├── job_evaluated.sql
│   ├── job_user_actions.sql
│   └── user_profiles.sql
│
├── gateway/                      ← LiteLLM proxy client factory & model aliases
│   ├── litellm_config.yaml       ← Model routing, fallbacks (Pro → Flash), rate caps
│   └── client.py                 ← AsyncInstructor client + embeddings helper
│                                    (embeddings must route through here too — see
│                                    AGENTS.md constraint #1 fix)
│
├── observability/                ← LangFuse OpenTelemetry setup & trace scoring
│   └── setup.py                  ← LangFuse client init + LiteLLM callback registration
│
├── services/                     ← Cloud Run microservices (each = one deployable)
│   ├── apify_receiver.py         ← Ingestion webhook consumer
│   ├── embedding_worker.py       ← [NEW] Pre-computes embeddings (text-embedding-004)
│   ├── user_profile_service.py   ← [NEW] Streamlit-facing profile write path
│   ├── tier1_matcher_runner.py   ← [NEW] Daily Cloud Scheduler entry point
│   ├── telegram_webhook.py       ← HITL inline-button callback receiver
│   └── reflection_runner.py      ← Weekly Cloud Scheduler entry point
│
├── utils/                        ← [NEW, scoped narrowly] Generic, domain-free helpers
│   │                                ONLY. If it encodes a business rule, it does not
│   │                                belong here — see retrieval/ or repositories/.
│   ├── ids.py                    ← job_id = ATS UUID or MD5(job_url)
│   ├── retry.py                  ← generic retry/backoff decorator
│   └── time.py                   ← timestamp/window helpers (e.g. the 90-day dedupe window)
│
├── config/                       ← Environment settings, constants, Secret Manager access
│   └── settings.py               ← Central Settings object & sandbox timeouts
│
├── tests/                        ← [NEW] Mirrors source tree
│   ├── agents/                   ← Agent logic w/ mocked LiteLLM (fixture-based)
│   ├── orchestration/            ← LoopAgent consensus-threshold regression tests
│   ├── retrieval/
│   └── repositories/             ← Against interfaces.py mocks, not live BigQuery
│
├── scripts/                      ← [NEW] One-off ops: embedding backfill, seed
│                                    Firestore dynamic_preference_rules, local bootstrap
│
├── infra/                        ← [NEW] Dockerfile per Cloud Run service, deploy config
│   ├── apify_receiver.Dockerfile
│   ├── embedding_worker.Dockerfile
│   ├── ... (one per services/*.py)
│   └── cloudbuild.yaml           ← or terraform/, if you go that route
│
├── .agents/                      ← AI coding agent context (rules, skills, workflows)
├── AGENTS.md
├── ARCHITECTURE.md
├── PROJECT_STRUCTURE.md          ← this file
├── pyproject.toml                ← [NEW] pytest/ruff/mypy config, once tests/ exists
└── requirements.txt
```

## Layering rule (the "single dependency" part)
Dependencies only point inward, never sideways or outward:

```
services/  (frameworks — Cloud Run entry points)
    ↓
orchestration/, agents/  (application/use-case logic)
    ↓
retrieval/, tools/  (domain logic)
    ↓
repositories/interfaces.py  (abstractions — agents depend on THIS)
    ↑ implements
repositories/*_repo.py  (concrete BigQuery adapters)
```

`schemas/` sits underneath everything — pure data contracts, no behavior, no imports
from `repositories/` or `gateway/`. Nothing above should import a concrete
`repositories/*_repo.py` class directly; go through `interfaces.py`.
