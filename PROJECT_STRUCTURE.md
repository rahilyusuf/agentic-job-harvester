# Repository structure and ownership

This is the file-location inventory, not a claim that every listed component is
complete or deployed. [ARCHITECTURE.md](ARCHITECTURE.md) owns behavior/status contracts;
[conventions.md](.agents/rules/conventions.md) owns dependency boundaries;
[AGENTS.md](AGENTS.md) owns instruction precedence. Update this inventory when modules
are added, removed, or renamed.

## Application modules — Implemented source

| Directory | Files and responsibility |
|---|---|
| `agents/` | `base.py`: abstract Python base; `router_agent.py`, `scorer_agent.py`, `skeptic_agent.py`: Tier 1; `research_agent.py`, `company_agent.py`, `redflag_agent.py`, `skillgap_agent.py`: research/diligence; `reflection_agent.py`: feedback synthesis. No implemented ADK runtime classes. |
| `orchestration/` | `pipeline.py`: explicit Python Tier1/Tier2 calls and dataclass results; `hitl_controller.py`: action coordination; `input_sanitizer.py`: normalization/prompt-boundary helpers; `output_validator.py`: model revalidation. |
| `retrieval/` | `vector_matcher.py`: interface-based candidate retrieval; `regex_prefilter.py`: title gate, optional extraction checks, candidate budget. |
| `schemas/` | `job_models.py`, `routing_models.py`, `scoring_models.py`, `research_models.py`, `diligence_models.py`, `reflection_models.py`, `profile_models.py`: Pydantic contracts. |
| `repositories/` | `interfaces.py`: protocols; `raw_jobs_repo.py`, `embeddings_repo.py`, `evaluated_repo.py`, `actions_repo.py`, `profiles_repo.py`: concrete BigQuery adapters. Named upserts/protocols have unresolved integrity gaps. |
| `gateway/` | `client.py`: Instructor chat wrapper and direct Vertex embedding adapter; `litellm_config.yaml`: configured chat aliases, fallbacks/callbacks/limits. Proxy embedding routing is not implemented. |
| `observability/` | `setup.py`: Langfuse initialization, decorator re-export, flush, metadata and scoring helpers. Privacy/correlation enforcement remains incomplete. |
| `tools/` | `search_tools.py`: Custom Search/page fetch; `cache_tools.py`: Firestore research cache; `history_tools.py`: current BigQuery history query; `e2b_tools.py`: sandbox execution. |
| `config/` | `settings.py`: environment/default settings and secret resolution. |
| `utils/` | `ids.py`, `retry.py`, `time.py`: generic identity, backoff and timestamp helpers. |

## Entry points and persistence

| Exact path | Implemented entry point / intended trigger |
|---|---|
| [services/apify_receiver.py](services/apify_receiver.py) | FastAPI `/webhook/apify`, `/health`, `/reconcile`, `/jobs/reconcile`; actor-event ingestion and publication recovery |
| [services/normalizers/apify_career_scraper.py](services/normalizers/apify_career_scraper.py) | Actor-specific item normalization, URL fallback and ingestion sanitization |
| [services/embedding_worker.py](services/embedding_worker.py) | FastAPI Pub/Sub push `/`, `/health`; consumes job embedding messages |
| [services/user_profile_service.py](services/user_profile_service.py) | FastAPI profile write endpoint and health route |
| [services/tier1_matcher_runner.py](services/tier1_matcher_runner.py) | Async `main()` and per-user runner; daily scheduling intent is Unverified |
| [services/telegram_webhook.py](services/telegram_webhook.py) | FastAPI Telegram callback and health routes |
| [services/reflection_runner.py](services/reflection_runner.py) | Async `main()`; weekly scheduling intent is Unverified |
| [sql/raw_job_postings.sql](sql/raw_job_postings.sql) | Intended raw job table DDL |
| [sql/job_embeddings.sql](sql/job_embeddings.sql) | Intended embeddings table/vector index DDL |
| [sql/job_evaluated.sql](sql/job_evaluated.sql) | Intended per-user evaluation table DDL |
| [sql/job_user_actions.sql](sql/job_user_actions.sql) | Intended action table DDL |
| [sql/user_profiles.sql](sql/user_profiles.sql) | Intended user profile table DDL |

A Python service file is not evidence of an independently deployed Cloud Run service.
A dedicated Tier 2 consumer and a Streamlit application are Planned, not present here.
Generic n8n support is also Planned.

## Infrastructure, operations and tests

- `infra/` contains `apify_receiver.Dockerfile`, `embedding_worker.Dockerfile`,
  `tier1_matcher_runner.Dockerfile`, `telegram_webhook.Dockerfile`,
  `reflection_runner.Dockerfile`, and `cloudbuild.yaml`. There is no Dockerfile for
  every service module. Cloud Build contains active deploy steps; live deployment is Unverified.
- `scripts/` contains `local_bootstrap.py`, `backfill_embeddings.py`, and
  `seed_preference_rules.py`. These may mutate external state; they are not documentation checks.
- `tests/agents/`, `tests/orchestration/`, `tests/retrieval/`, `tests/repositories/`,
  and `tests/services/` hold corresponding regression tests. Existing service tests
  cover normalization, ingestion auth/provenance, publication failure/recovery and
  worker retries. Test presence does not imply a passing run or deployed verification.
- `requirements.txt` declares dependency ranges; `pyproject.toml` declares Python
  compatibility, package discovery and development-tool settings. Neither is proof
  of the installed environment.

## Governance inventory

```text
AGENTS.md                           instruction hierarchy and policy ownership
ARCHITECTURE.md                     implemented/planned/unverified design and contracts
PROJECT_STRUCTURE.md                this inventory
README.md                           project entry point
.agents/
  rules/
    conventions.md                  layer and implementation conventions
    guardrails.md                   canonical security/privacy/cost requirements
    stack.md                        dependency and model configuration evidence
    remediation-backlog.md          informational unresolved findings and decisions
  skills/
    pipeline-orchestration/SKILL.md  current Python pipeline procedures
    bq-vector-search/SKILL.md        retrieval/vector/index procedures
    e2b-sandboxing/SKILL.md          generated-code sandbox procedures
    input-sanitization/SKILL.md      external-content trust boundaries
    langfuse-tracing/SKILL.md        privacy-safe tracing/correlation procedures
    litellm-instructor/SKILL.md      gateway/structured-output procedures
  workflows/
    add-agent.md                    adding an authorized Python agent
    debug-score-debate.md           investigating debate behavior
```

The root duplicate sanitization skill and misleading ADK-named skill/workflow have
been replaced in this documentation alignment. Skills own procedures, not another
copy of policy, schema, model mapping, or lifecycle contracts.
