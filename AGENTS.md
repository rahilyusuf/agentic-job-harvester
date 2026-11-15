# AGENTS.md — Agentic Job Harvester

Read this file before working in this repository. The project targets job intelligence
for a 1–3 YOE AI/ML engineer in Saudi Arabia and the UAE. The current implementation
uses Python async orchestration, BigQuery, Pub/Sub, Instructor/LiteLLM, and external
tools. Google ADK integration is planned, not the current orchestration runtime.

## Instruction hierarchy

Apply instructions in this order, highest first:

1. Platform/system instructions, then developer instructions.
2. Explicit user instructions for the current task and continuing session.
3. Applicable `AGENTS.md` files; a more specific nested file governs its subtree.
4. Canonical requirements in `.agents/rules/`, within the ownership table below.
5. Normative design requirements in [ARCHITECTURE.md](ARCHITECTURE.md).
6. File placement and responsibility guidance in [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md).
7. An activated skill's task-specific procedures.
8. Workflows, examples, comments, and historical material.

Lower-priority material cannot override higher-priority requirements. At the same
priority, use the designated policy owner; report unresolved conflicts before taking
an action that depends on them. Skills and workflows do not authorize additional work,
provider changes, deployments, or migrations.

Instruction authority and implementation evidence are different. Source code shows
what is implemented; tests show only the behavior they exercise; runtime claims need
deployment evidence. A mismatch is a recorded gap, not permission to weaken a rule or
rewrite a working pipeline to resemble an outdated design.

## Policy ownership

| Owner | Responsibility |
|---|---|
| This file | Instruction precedence, document ownership, task routing |
| [guardrails.md](.agents/rules/guardrails.md) | Security, privacy, cost, isolation, durable-data requirements |
| [conventions.md](.agents/rules/conventions.md) | Dependency boundaries, async I/O, serialization and change conventions |
| [stack.md](.agents/rules/stack.md) | Runtime/dependency evidence and model configuration references |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Implemented/planned/unverified architecture, function/state contracts, lifecycle ownership |
| [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) | Actual repository inventory and module locations |
| Skills | Scoped implementation procedures; refer to canonical requirements |
| Workflows | Task/check sequences; refer to skills and policy owners |
| [remediation-backlog.md](.agents/rules/remediation-backlog.md) | Informational unresolved findings, dependencies, and risks; no policy overrides |

Executable contracts are `schemas/` for model validation, `repositories/interfaces.py`
for repository protocols, `sql/` for intended table DDL, and
`gateway/litellm_config.yaml` for configured chat aliases/provider mappings. They do
not prove live schema/configuration or correct enforcement. Record disagreements in
the backlog and update contracts together when implementation work is authorized.

## Working rules

Read the canonical guardrails and conventions before changing implementation. Preserve
the existing Apify normalization/authentication/provenance checks, BigQuery writes,
Pub/Sub publication recovery, and embedding pipeline. Architecture or provider changes
require explicit scope; documentation alignment alone authorizes neither.

Model aliases are defined by gateway configuration, not historical prose. The current
direct Vertex embedding adapter is an unresolved deviation from the proxy-only target;
see [model access](ARCHITECTURE.md#model-access). Do not expand that deviation or
silently approve it as permanent.

When behavior, a schema, a boundary, or a module changes, update its owning document
and relevant evidence links in the same change. Keep normative rules in their owner
instead of copying them into skills or this file.

## Skills and workflows

Read the relevant skill before implementing work in its scope:

| Task | Skill |
|---|---|
| Python sequencing, debate, Tier 2 coordination | [pipeline-orchestration](.agents/skills/pipeline-orchestration/SKILL.md) |
| BigQuery retrieval, vectors, index evaluation | [bq-vector-search](.agents/skills/bq-vector-search/SKILL.md) |
| Generated challenge execution in E2B | [e2b-sandboxing](.agents/skills/e2b-sandboxing/SKILL.md) |
| Langfuse capture, correlation, callbacks, scoring | [langfuse-tracing](.agents/skills/langfuse-tracing/SKILL.md) |
| Gateway, Instructor, aliases, embeddings integration | [litellm-instructor](.agents/skills/litellm-instructor/SKILL.md) |
| External content normalization and prompt boundaries | [input-sanitization](.agents/skills/input-sanitization/SKILL.md) |

Use [add-agent](.agents/workflows/add-agent.md) for an authorized new agent and
[debug-score-debate](.agents/workflows/debug-score-debate.md) for debate regressions.
An ADK migration is not a prerequisite for either workflow.
