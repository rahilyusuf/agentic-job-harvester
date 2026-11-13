"""agents — ADK agent classes (one file per agent).

All agents inherit from BaseIntelligenceAgent which wires:
  - Instructor + LiteLLM proxy (via gateway/client.py)
  - LangFuse @observe tracing (via observability/setup.py)
  - Pydantic v2 output validation

Import only from repositories/interfaces.py — never from concrete *_repo.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agents.base import BaseIntelligenceAgent
    from agents.company_agent import CompanyVerificationAgent
    from agents.redflag_agent import RedFlagDetectorAgent
    from agents.reflection_agent import ReflectionAgent
    from agents.research_agent import CompanyResearchAgent
    from agents.router_agent import JobIntakeRouter
    from agents.scorer_agent import ScorerAgent
    from agents.skeptic_agent import SkepticAgent
    from agents.skillgap_agent import SkillGapAgent

__all__ = [
    "BaseIntelligenceAgent",
    "CompanyResearchAgent",
    "CompanyVerificationAgent",
    "JobIntakeRouter",
    "RedFlagDetectorAgent",
    "ReflectionAgent",
    "ScorerAgent",
    "SkepticAgent",
    "SkillGapAgent",
]

_EXPORTS = {
    "BaseIntelligenceAgent": ("agents.base", "BaseIntelligenceAgent"),
    "JobIntakeRouter": ("agents.router_agent", "JobIntakeRouter"),
    "ScorerAgent": ("agents.scorer_agent", "ScorerAgent"),
    "SkepticAgent": ("agents.skeptic_agent", "SkepticAgent"),
    "CompanyResearchAgent": ("agents.research_agent", "CompanyResearchAgent"),
    "CompanyVerificationAgent": ("agents.company_agent", "CompanyVerificationAgent"),
    "RedFlagDetectorAgent": ("agents.redflag_agent", "RedFlagDetectorAgent"),
    "SkillGapAgent": ("agents.skillgap_agent", "SkillGapAgent"),
    "ReflectionAgent": ("agents.reflection_agent", "ReflectionAgent"),
}


def __getattr__(name: str) -> Any:
    """Lazy-load agent classes to avoid circular imports during startup."""
    if name in _EXPORTS:
        module_name, attr_name = _EXPORTS[name]
        module = __import__(module_name, fromlist=[attr_name])
        return getattr(module, attr_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

