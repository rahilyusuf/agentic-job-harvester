"""agents — ADK agent classes (one file per agent).

All agents inherit from BaseIntelligenceAgent which wires:
  - Instructor + LiteLLM proxy (via gateway/client.py)
  - LangFuse @observe tracing (via observability/setup.py)
  - Pydantic v2 output validation

Import only from repositories/interfaces.py — never from concrete *_repo.py.
"""

from agents.base import BaseIntelligenceAgent
from agents.router_agent import JobIntakeRouter
from agents.scorer_agent import ScorerAgent
from agents.skeptic_agent import SkepticAgent
from agents.research_agent import CompanyResearchAgent
from agents.company_agent import CompanyVerificationAgent
from agents.redflag_agent import RedFlagDetectorAgent
from agents.skillgap_agent import SkillGapAgent
from agents.reflection_agent import ReflectionAgent

__all__ = [
    "BaseIntelligenceAgent",
    "JobIntakeRouter",
    "ScorerAgent",
    "SkepticAgent",
    "CompanyResearchAgent",
    "CompanyVerificationAgent",
    "RedFlagDetectorAgent",
    "SkillGapAgent",
    "ReflectionAgent",
]
