# Conventions and dependency boundaries

This file owns implementation conventions under the [instruction hierarchy](../../AGENTS.md#instruction-hierarchy).
Security and cost requirements belong to [guardrails.md](guardrails.md); architectural
facts and lifecycle contracts belong to [ARCHITECTURE.md](../../ARCHITECTURE.md).

## Layer responsibilities

| Layer | Owns | Dependencies and restrictions |
|---|---|---|
| `services/` | Transport authentication/validation, lifecycle, composition, entry points | May instantiate concrete repositories and inject them into use cases. HTTP and Pub/Sub mechanics belong here or in injected adapters. |
| `orchestration/` | Sequencing, debate, eligibility, concurrency, validation, HITL coordination | Uses agents, schemas, repository interfaces, injected capabilities; no concrete repository imports. Existing HITL transport coupling is backlog, not a template. |
| `agents/` | Prompt construction and typed intelligence operations | Uses `BaseIntelligenceAgent`, approved tools, schemas, repository interfaces. No concrete repositories or HTTP request handlers. |
| `retrieval/` | Candidate selection and prefilter business logic | Uses schemas and repository interfaces, not concrete BigQuery adapters. |
| `repositories/` | Persistence, SQL, serialization and storage concurrency | Protocols in `interfaces.py`; concrete adapters implement them. No model calls. |
| `gateway/` | Model access, provider adapters, aliases, retries, request metadata | Agent base and embedding producers may use it. No model access from schemas/repositories. |
| `observability/` | Safe trace lifecycle, correlation and feedback scoring | May be used across layers. No business decisions or unrestricted prompt capture. |
| `tools/` | Bounded search/fetch/cache/history/sandbox capabilities | Database operations should use repository interfaces. Current history SQL and Firestore clients are documented coupling; Firestore remains limited by G-8. |
| `schemas/` | Pure Pydantic contracts and validation | No project persistence, gateway, or network dependencies. |
| `config/` | Settings and secret resolution | Central configuration boundary; no business decisions. |
| `utils/` | Generic IDs, time and retry helpers | No scoring/retrieval business policy. |

Dependencies follow responsibilities, not a blanket ban on sibling imports. Agents
can call approved tools and the base can call the gateway. Composition is the explicit
exception allowing services to construct concrete adapters; business logic still
depends on protocols.

## Async I/O and data boundaries

- Agent entry points and I/O paths are async. Use `httpx.AsyncClient` and
  `asyncio.sleep`; no blocking HTTP or `time.sleep` in agent execution. Synchronous
  SDK work must be bounded and offloaded at an adapter boundary. An `async def`
  around `query_and_wait()` does not make that SDK call nonblocking.
- Pass typed models and explicit arguments between use cases. Pydantic `model_dump`
  dictionaries are valid serialized boundary representations after validation;
  unvalidated LLM dictionaries are not. See G-3 and the architecture contracts.
- Do not introduce ADK session keys, `output_key`, or a generic state dictionary to
  satisfy old examples. New shared-state contracts require an architecture update.
- Separate global ingestion/embedding state from per-user evaluation state when
  designing changes. Do not silently migrate current states during unrelated work.
- Use existing filenames and symbols. Keep examples executable against supported
  interfaces or label them explicitly as design sketches.

## Review and verification

Keep changes scoped and reviewable. Preserve relevant existing tests; use mocked
repositories/model/transport clients for unit verification. Live integration tests,
cloud scripts, and deployment commands need appropriate task authorization. Never
import application settings during documentation checks: settings may access secrets.

Record architecture claims as Implemented, Planned, or Unverified using the definitions
in the architecture. Update structure documentation when adding/removing/renaming
modules. Documentation changes must not imply a security finding has been fixed.
