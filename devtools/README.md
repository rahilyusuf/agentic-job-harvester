# Local development API

Production continues to use `services.apify_receiver:app`. It never imports this
package. All production Docker builds exclude `devtools/` and `tests/devtools/`
through the root `.dockerignore`. No cloud resources are created by these tools.

Use the separate local launcher from the repository root. It reuses existing
production HTTP handlers but does not run their telemetry/Secret Manager startup.
Calling a production webhook still has its normal production behavior.

## Start locally (PowerShell)

Use existing Application Default Credentials authorized for the approved resources.
Do not put credentials or the development token into source files or URLs.

```powershell
$env:ENVIRONMENT = "development"
$env:ENABLE_DEV_ENDPOINTS = "true"
$env:GCP_PROJECT_ID = "agentic-job-harvester-v2"
$env:BQ_DATASET = "job_intelligence"
$env:BQ_LOCATION = "US"
$env:PUBSUB_EMBEDDING_TOPIC = "job-embedding-queue"
$env:DEV_PUBSUB_SUBSCRIPTION = "job-embedding-test-sub"
$devToken = Read-Host "Development token (at least 32 UTF-8 bytes)" -AsSecureString
$env:DEV_AUTH_SECRET = [System.Net.NetworkCredential]::new("", $devToken).Password
$env:LITELLM_LOCAL_MODEL_COST_MAP = "True"
$env:LANGFUSE_TRACING_ENABLED = "false"
.\venv\Scripts\python.exe -m devtools
```

The launcher binds `127.0.0.1:8081` and disables access logging. Do not expose it
through a proxy or change the binding. A missing/unrecognized environment, missing
enable flag, short/missing secret, or Cloud Run `K_SERVICE` fails closed. These
settings are explicit process variables; the dev gate never reads `.env` or uses
production Settings defaults. Secret rotation is checked on each request.

## Swagger and Postman

Open `http://127.0.0.1:8081/docs`. Use Swagger's Authorize button for `X-Dev-Token`.
In Postman, use base URL `http://127.0.0.1:8081` and header
`X-Dev-Token: {{dev_secret}}` for all development requests. Keep the secret in a
private local variable; do not export it in a collection.

| Method and path | Body | Effects |
|---|---|---|
| `GET /dev/health` | None | Local only; no cloud clients |
| `GET /dev/gcp/status` | None | Read-only dataset/table/topic/subscription metadata |
| `POST /dev/ingestion/preview` | `{"limit":3}` | Local fixture normalization/selection; no cloud access |
| `POST /dev/ingestion/execute` | `{"limit":3,"confirmation":"WRITE_THREE_OR_FEWER_TEST_JOBS"}` | Real repository insertion and publication of confirmed inserted IDs only |
| `GET /dev/pubsub/status` | None | Read-only topic/subscription metadata; no pull or acknowledge |

Requests cannot supply resource IDs, fixture paths or arbitrary SQL. Selection uses
the unchanged 215-row fixture, production normalization and first-valid-occurrence
deduplication, then the first one to three unique jobs. Preview returns summaries,
not descriptions or credential-bearing configuration. There is no embedding endpoint.

## Write safeguards and limits

The exact project/dataset/table/topic/subscription are fixed in `config.py` from the
approved resource list. Metadata must confirm the live dataset location `US`, a real
table, an ACTIVE pull-only subscription, a 60-second acknowledgment deadline and
24-hour retention. The test subscription must be the topic's **only** subscription,
with no filter, push delivery or export sink. If an embedding-worker subscription is
attached, status/execute fail closed. Topic safety is checked again before publication.

Execute uses the actual `RawJobRepository` with explicit settings injection and its
one-attempt entry point. It disables SDK insert retries and bypasses the normal outer
retry policy. A timeout, partial failure or invalid insertion confirmation returns
502 and publishes nothing. Some rows may already be stored: inspect them before any
manual retry. A successful response can still report publication failures; it is not
an exactly-once guarantee and does not automatically retry or run recovery.

Metadata checks cannot detect another consumer pulling the approved test subscription,
nor prevent an administrator attaching another subscription after verification. Reserve
the test subscription for this session and restrict who can change topic subscriptions.
This implementation never calls Vertex or an LLM, but do not run execute if the topic
is being consumed by an embedding worker. Existing production ingestion retry/recovery
semantics are unchanged.

Automated tests override adapters, metadata and settings and deny external connections.
Do not use live endpoint calls as a replacement for those tests.
