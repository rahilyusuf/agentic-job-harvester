# Agentic Job Harvester 🚀

> Multi-agent job evaluation platform built on **Google ADK**, **BigQuery Vector Search**, **LiteLLM**, **Langfuse**, and **E2B MicroVM Sandboxes**.

## 🏗 Architecture Overview

The system runs a 4-tier closed-loop pipeline:

1. **Ingestion & Pre-Computation:** Cloud Run services (`apify-receiver` & `job-embedding-worker`) digest webhooks, deduplicate against a 90-day window, and generate 768-dim vector embeddings (`text-embedding-004`).
2. **Tier 1 Agentic Scoring:** 
   - BigQuery Cosine Vector Search (`ML.DISTANCE`) + deterministic regex pre-filter ($0 cost).
   - `JobIntakeRouter` (`SKIP` | `PROCESS` | `HIGH_PRIORITY`).
   - `ScoreDebateLoop` (ADK `LoopAgent` running `ScorerAgent` vs. `SkepticAgent` consensus audit with final veto authority).
3. **Tier 2 On-Demand Intelligence Swarm:**
   - `CompanyResearchAgent` (ReAct pattern with Google Search and cache tools).
   - `DueDiligenceSuite` (ADK `ParallelAgent` running verification, ghost job/toxicity scans, and skill gap detection).
   - `SkillGapAgent` executes code challenges inside an isolated **E2B MicroVM Sandbox**.
4. **HITL & Active Learning Flywheel:** Telegram inline actions (`Apply`, `Pass`) record user telemetry into BigQuery and tag Langfuse traces, driving weekly active learning updates to the user's preference centroid.

## 🛠 Tech Stack

- **Orchestration:** Google ADK (`google-adk`)
- **LLM Gateway:** LiteLLM Proxy (RPM token buckets, model fallbacks)
- **Vector Engine & Storage:** BigQuery Native Vector Index + Firestore
- **Code Sandboxing:** E2B (`e2b-code-interpreter`)
- **Observability:** Langfuse (OpenTelemetry tracing & feedback scoring)
- **Validation:** Instructor + Pydantic v2

## 📁 Repository Blueprint

Detailed folder hierarchy and dependency rules are defined in [`PROJECT_STRUCTURE.md`](./PROJECT_STRUCTURE.md) and [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## 🚦 Quickstart Setup

```bash
# 1. Clone repository
git clone [https://github.com/](https://github.com/)<your-github-username>/agentic-job-harvester.git
cd agentic-job-harvester

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install Google ADK and core dependencies
pip install --upgrade pip
pip install google-adk instructor litellm langfuse e2b-code-interpreter

# 4. Copy environment environment configuration
cp .env.example .env

# 5. Launch local ADK dev web server
adk web agents/
