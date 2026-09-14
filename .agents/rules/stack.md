# Stack & Version Pins — agentic-job-harvester

## Runtime Versions
- **Python**: 3.11+ (required for `asyncio.TaskGroup`, `tomllib`)
- **google-adk**: `>=0.4.0,<0.5.0` — ADK SequentialAgent / LoopAgent / ParallelAgent / LlmAgent
- **google-genai**: `>=1.11.0` — underlying GenAI SDK (do NOT call directly; route via gateway/)
- **google-cloud-bigquery**: `>=3.25.0` — BQ client; use `query_and_wait` for sync, `create_job` async
- **google-cloud-pubsub**: `>=2.20.0` — PublisherClient for `job-embedding-queue`
- **google-cloud-firestore**: `>=2.16.0` — dynamic_preference_rules config cache + research cache
- **google-cloud-secret-manager**: `>=2.20.0` — all secrets must be read via SecretManagerServiceClient
- **google-cloud-aiplatform**: `>=1.70.0` — text-embedding-004 via `aiplatform.init` (embedding worker only)
- **fastapi**: `>=0.111.0` — Cloud Run service entry points
- **uvicorn**: `>=0.30.0` — ASGI server (with `--workers 1` per container)
- **pydantic**: `>=2.7.0,<3.0.0` — all schemas; use `model_validator`, `field_validator`
- **instructor**: `>=1.3.0` — structured outputs; always use `instructor.from_litellm()`
- **litellm**: `>=1.40.0` — proxy client; configure via `gateway/litellm_config.yaml`
- **e2b-code-interpreter**: `>=1.0.0` — `AsyncSandbox` for SkillGapAgent code challenges
- **langfuse**: `>=2.35.0` — `@observe` decorator, `langfuse.score()` for HITL telemetry
- **python-dotenv**: `>=1.0.1` — local `.env` loading in `config/settings.py`

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
- Input format: `f"{title} {company} {location} {description[:2000]}"`

## Cloud Run Services
| Service | Entry Point | Trigger | Min Instances |
|---|---|---|---|
| apify-receiver | `services/apify_receiver.py` | Pub/Sub push / HTTP | 0 |
| embedding-worker | `services/embedding_worker.py` | Pub/Sub `job-embedding-queue` | 0 |
| tier1-matcher | `services/tier1_matcher_runner.py` | Cloud Scheduler (daily) | 0 |
| telegram-webhook | `services/telegram_webhook.py` | HTTP (Telegram Bot API) | 1 |
| reflection-runner | `services/reflection_runner.py` | Cloud Scheduler (weekly) | 0 |

## BigQuery Dataset
- Dataset ID: `${BQ_DATASET}` (env var; default `job_intelligence`)
- Project: `${GCP_PROJECT_ID}`
- Location: `${BQ_LOCATION}` (default `US`)
