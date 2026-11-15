# Stack and configuration evidence

This file owns dependency/configuration guidance under [AGENTS.md](../../AGENTS.md).
Security requirements are in [guardrails.md](guardrails.md), not redefined here.

## Runtime and dependencies

**Implemented declarations:** [pyproject.toml](../../pyproject.toml) requires Python
3.11+ and configures pytest, Ruff, mypy, and packaging. [requirements.txt](../../requirements.txt)
declares runtime packages, mostly with lower bounds; it is not an exact lockfile.
Use those manifests rather than copying a second set of version pins into instructions.

The application uses FastAPI/httpx, Google Cloud BigQuery/Pub/Sub/Firestore/Secret
Manager/AI Platform SDKs, Pydantic v2, Instructor, LiteLLM, Langfuse, NumPy, and E2B.
Google ADK and Google GenAI packages are declared but do not establish runtime ADK use.
ADK integration is **Planned** and requires a separate architecture decision.

**Unverified:** installed/deployed versions, compatibility of broad package ranges,
Langfuse callback/API behavior, E2B lifecycle API, and cloud SDK behavior under the
deployed runtime. Comments naming an installed SDK version are not an environment
inventory. Do not install/upgrade packages as part of documentation alignment.

## Model configuration

[gateway/litellm_config.yaml](../../gateway/litellm_config.yaml) owns configured chat
aliases and provider mappings. Update this evidence snapshot when that file changes:

| Alias | Configured provider model | Current use |
|---|---|---|
| `router-flash` | `gemini/gemini-2.5-flash` | Intake router |
| `fast-flash` | `gemini/gemini-2.5-flash` | Scoring, skepticism, research, verification, red flags, skill gaps |
| `deep-pro` | `gemini/gemini-2.5-pro` | Reflection |

Use aliases at chat call sites. The configuration describes Gemini API routing, not
`vertex_ai/` chat routing. It declares a Pro-to-Flash fallback, a Flash self-fallback,
and global limits/budget; these do not prove effective enforcement or a dedicated
10-RPM Pro cap. Actual client-to-proxy routing is **Unverified**.

[gateway/client.py](../../gateway/client.py) owns the existing embedding implementation:
`text-embedding-004`, 768 dimensions, direct Vertex prediction, executor-offloaded.
There is no registered embedding alias in the proxy configuration. This is the unresolved
G-1 deviation, not permission to add direct calls or a permanent routing decision.

## Configuration and deployability

[config/settings.py](../../config/settings.py) is the settings/secret boundary.
Defaults include dataset `job_intelligence`, BigQuery location `US`, region
`us-central1`, and embedding topic `job-embedding-queue`; deployed values are **Unverified**.
The embedding adapter also hardcodes a Vertex region; see A-1 in the backlog.

Use [PROJECT_STRUCTURE.md](../../PROJECT_STRUCTURE.md) for entry points and infrastructure.
Daily/weekly schedules and service minimum instances are intent unless verified in
deployment. `infra/cloudbuild.yaml` contains active deploy steps despite its optional
comment; it is not a read-only validation command. This guidance does not authorize it.
