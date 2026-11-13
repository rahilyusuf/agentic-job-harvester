---
name: adk-orchestration
description: Guidance on ADK (Agent Development Kit) multi-agent orchestration patterns in this repository, including SequentialAgent, LoopAgent (debate loop), ParallelAgent (due diligence suite), and ReAct LlmAgent. Covers agent lifecycles, dependency injection, and state conventions.
---

# ADK Multi-Agent Orchestration Patterns

This skill guides the design, implementation, and modification of ADK-based agent patterns across `agentic-job-harvester-v2`.

---

## 1. Agent Architecture & Base Class

All agents inherit from `BaseIntelligenceAgent` (`agents/base.py`), which standardizes:
- **Instructor + LiteLLM client routing** via `gateway/client.py`.
- **LangFuse observability** via the `@observe` decorator on execution methods.
- **Pydantic v2 output schema validation** on all LLM responses.

```python
from agents.base import BaseIntelligenceAgent
from schemas.scoring_models import ScorerOutput

class ScorerAgent(BaseIntelligenceAgent):
    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = ScorerOutput

    def _system_prompt(self) -> str:
        return "..."

    def _build_messages(self, ...) -> list[dict[str, str]]:
        return [...]
```

---

## 2. Core Orchestration Patterns

### A. Dynamic Routing (`JobIntakeRouter`)
- **Location:** `agents/router_agent.py`
- **Pattern:** Sequential entry gate. Reads Firestore dynamic preference rules.
- **Decisions:** `SKIP`, `PROCESS`, `HIGH_PRIORITY`.
- **Cost discipline:** If `decision == SKIP`, halt pipeline immediately (~$0.0001 cost).

### B. Multi-Agent Debate Loop (`ScoreDebateLoop`)
- **Location:** `orchestration/pipeline.py` (Tier 1 Step E)
- **Pattern:** `LoopAgent` wiring `ScorerAgent` $\leftrightarrow$ `SkepticAgent`.
- **Constraint:** Hard cap of `max_iterations = 2`. Never omit this limit.
- **Consensus Rule:**
  - Round 1: Scorer scores fit (0-100); Skeptic audits for disqualifiers (ghost jobs, toxicity).
  - If `|scorer_score - skeptic_agreed_score| > threshold` (default 10) and not vetoed $\rightarrow$ trigger Round 2.
  - Round 2: Scorer revises fit based on Skeptic feedback; Skeptic makes final determination.
- **Veto Authority:** The Skeptic holds final veto authority. If `veto=True`, treat as `SKIP`.

### C. ReAct Pattern (`CompanyResearchAgent`)
- **Location:** `agents/research_agent.py` (Tier 2 Step 1)
- **Pattern:** ADK `LlmAgent` using ReAct with tool execution.
- **Constraint:** `max_iterations = 5`.
- **Allowed Tools:**
  1. `lookup_research_cache` (Firestore cache read)
  2. `google_search` (Search API)
  3. `fetch_url` (Page retrieval; content MUST be sanitized via `orchestration/input_sanitizer.py`)
  4. `check_application_history` (BigQuery historical lookup)

### D. Parallel Agent Suite (`DueDiligenceSuite`)
- **Location:** `orchestration/pipeline.py` (Tier 2 Step 2)
- **Pattern:** `ParallelAgent` running 3 concurrent sub-agents after company research completes:
  - `CompanyVerificationAgent` (headcount, funding, entity type)
  - `RedFlagDetectorAgent` (ghost jobs, Glassdoor toxicity, unrealistic demands)
  - `SkillGapAgent` (blocking vs. learnable gaps + E2B practice challenge generation)

---

## 3. Session State Conventions

All agents read from and write to session state using the exact keys defined in `AGENTS.md`:

| State Key | Model Type | Description |
|---|---|---|
| `current_job` | `RawJob.model_dump()` | Raw ingested job details |
| `resume_text` | `str` | Candidate resume (NEVER logged or stored in BQ) |
| `preference_rules` | `list[dict]` | Firestore dynamic rules |
| `router_decision` | `RouterDecision.model_dump()` | Intake routing output |
| `scorer_output` | `ScorerOutput.model_dump()` | Fit scoring output |
| `skeptic_output` | `SkepticOutput.model_dump()` | Audit and agreed score |
| `company_research` | `CompanyResearch.model_dump()` | ReAct research findings |
| `company_profile` | `CompanyProfile.model_dump()` | Verified company stats |
| `red_flag_analysis` | `RedFlagAnalysis.model_dump()` | Identified risks |
| `skill_gap_report` | `SkillGapReport.model_dump()` | Gaps and practice challenge |
| `langfuse_trace_id` | `str` | Trace correlation ID |

---

## 4. Implementation Rules & Best Practices

1. **Dependency Inversion:** Agents and pipelines MUST import repository interfaces from `repositories/interfaces.py` (`IRawJobRepository`, `IEvaluatedRepository`), never concrete `*_repo.py` classes.
2. **Pure Async:** All agent methods must be `async`. Use `httpx.AsyncClient` for network I/O; never use `requests` or blocking `time.sleep()`.
3. **No Direct GenAI Calls:** Always use `self._call_llm()` or `gateway/client.py`.
4. **Input Sanitization:** Untrusted external scraped content (descriptions, fetched URLs) must pass through `format_job_for_prompt()` or `sanitize_scraped_text()` before entering LLM messages.
5. **Output Validation:** Pipe all agent outputs through `orchestration/output_validator.py` (`OutputValidator.validate()`).

