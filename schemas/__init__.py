"""schemas — Pure Pydantic v2 data contracts.

No imports from repositories/, gateway/, agents/, or any other project module.
This package is the single source of truth for all inter-agent data shapes.
"""

from schemas.job_models import EvaluatedJob, JobStructuredExtraction, RawJob
from schemas.routing_models import RouterDecision, RoutingDecision
from schemas.scoring_models import SkepticOutput, ScorerOutput
from schemas.research_models import CompanyProfile, CompanyResearch
from schemas.diligence_models import CodeChallenge, RedFlagAnalysis, SkillGapReport
from schemas.reflection_models import PreferenceRule, PreferenceRules
from schemas.profile_models import ResumeMetadata, UserProfile

__all__ = [
    # job
    "RawJob",
    "JobStructuredExtraction",
    "EvaluatedJob",
    # routing
    "RouterDecision",
    "RoutingDecision",
    # scoring
    "ScorerOutput",
    "SkepticOutput",
    # research
    "CompanyResearch",
    "CompanyProfile",
    # diligence
    "RedFlagAnalysis",
    "SkillGapReport",
    "CodeChallenge",
    # reflection
    "PreferenceRule",
    "PreferenceRules",
    # profile
    "UserProfile",
    "ResumeMetadata",
]
