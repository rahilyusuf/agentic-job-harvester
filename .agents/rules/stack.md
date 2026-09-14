# Stack & Version Pins — agentic-job-harvester

## Runtime Versions
- **Python**: 3.12 or 3.13, pinned. Not 3.14 (wheel availability lag for compiled deps).
- **google-adk**: `>=2.8.0,<3.0.0` — ADK SequentialAgent / LoopAgent / ParallelAgent / LlmAgent
- **google-genai**: `>=1.11.0` — underlying GenAI SDK (do NOT call directly; route via gateway/)
- **google-cloud-bigquery**: `>=3.25.0` — BQ client; use `query_and_wait` for sync, `create_job` async
- **google-cloud-pubsub**: `>=2.20.0` — PublisherClient for `job-embedding-queue`
- **google-cloud-firestore**: `>=2.16.0` — dynamic_preference_rules config cache + research cache
- **google-cloud-secret-manager**: `>=2.20.0` — all secrets must be read via SecretManagerServiceClient
- **fastapi**: `>=0.111.0` — Cloud Run service entry points
- **uvicorn**: `>=0.30.0` — ASGI server (with `--workers 1` per container)
- **pydantic**: `>=2.7.0,<3.0.0` — all schemas; use `model_validator`, `field_validator`
- **pydantic-settings**: `>=2.3.0` — `BaseSettings` subclass in `config/settings.py`; binds env vars and Secret Manager values
- **instructor**: `>=1.3.0` — structured outputs; always use `instructor.from_litellm()`
- **litellm**: `>=1.40.0` — proxy client; configure via `gateway/litellm_config.yaml`
- **e2b-code-interpreter**: `>=1.0.0` — `AsyncSandbox` for SkillGapAgent code challenges
- **langfuse**: `>=2.35.0` — `@observe` decorator, `langfuse.score()` for HITL telemetry
- **python-dotenv**: `>=1.0.1` — local `.env` loading in `config/settings.py`
- **httpx**: `>=0.27.0` — async HTTP client; required by AGENTS.md constraint #4 (no `requests.get` in agents)
- **numpy**: `>=1.26.0` — vector arithmetic for centroid-drift formula in `ReflectionAgent` (`e_active_new = (1-α)*e_resume + α*e_accepted_centroid`)

## Model Aliases (configured in gateway/litellm_config.yaml)
| Alias | Resolves To | Use Case |
|---|---|---|
| `fast-flash` | `gemini/gemini-2.5-flash` | ScorerAgent, SkepticAgent, CompanyVerificationAgent, RedFlagDetectorAgent, SkillGapAgent |
| `deep-pro` | `gemini/gemini-2.5-pro` | ReflectionAgent (weekly, cost-tolerant) |
| `router-flash` | `gemini/gemini-2.5-flash` | JobIntakeRouter (cheapest viable model) |

## LiteLLM Proxy Constraints
- Base URL: `${LITELLM_PROXY_BASE_URL}` (env var, never hardcoded)
- API key from Secret Manager: `litellm-proxy-api-key`
- All calls must pass `metadata={"trace_id": langfuse_trace_id}` for correlation

## Embedding Model
- Model: `text-embedding-004` (768 dimensions)
- Entry point: `gateway/client.py::generate_embedding()` — the ONLY place this is called
- Routing: via `litellm.aembedding()` through the LiteLLM proxy. Application code never
  imports `google-cloud-aiplatform` — LiteLLM authenticates to Vertex AI server-side.
- Input format: `f"{title} {company} {location} {description[:2000]}"`

## Cloud Run Services
| Service | Entry Point | Trigger | Min Instances |
|---|---|---|---|
| apify-receiver | `services/apify_receiver.py` | Pub/Sub push / HTTP | 0 |
| embedding-worker | `services/embedding_worker.py` | Pub/Sub `job-embedding-queue` | 0 |
| tier1-matcher | `services/tier1_matcher_runner.py` | Cloud Scheduler (daily) | 0 |
| telegram-webhook | `services/telegram_webhook.py` | HTTP (Telegram Bot API) | 1 |
| reflection-runner | `services/reflection_runner.py` | Cloud Scheduler (weekly) | 0 |
| user-profile-service | `services/user_profile_service.py` | HTTP (Streamlit frontend) | 0 |

## BigQuery Dataset
- Dataset ID: `${BQ_DATASET}` (env var; default `job_intelligence`)
- Project: `${GCP_PROJECT_ID}`
- Location: `${BQ_LOCATION}` (default `US`)
