"""schemas/research_models.py — CompanyResearch & CompanyProfile contracts.

CompanyResearchAgent (ReAct, Tier 2) produces a CompanyResearch.
CompanyVerificationAgent (ParallelAgent sub-agent) produces a CompanyProfile.
Both are cached in Firestore under `research_cache/{company_normalized}`.
"""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl


class FundingStage(str, enum.Enum):
    BOOTSTRAPPED = "BOOTSTRAPPED"
    PRE_SEED = "PRE_SEED"
    SEED = "SEED"
    SERIES_A = "SERIES_A"
    SERIES_B = "SERIES_B"
    SERIES_C_PLUS = "SERIES_C_PLUS"
    PUBLIC = "PUBLIC"
    UNKNOWN = "UNKNOWN"


class EntityType(str, enum.Enum):
    MNC = "MNC"
    VC_STARTUP = "VC_STARTUP"
    BOOTSTRAPPED_STARTUP = "BOOTSTRAPPED_STARTUP"
    STAFFING_AGENCY = "STAFFING_AGENCY"
    CONSULTING_FIRM = "CONSULTING_FIRM"
    GOV_ADJACENT = "GOV_ADJACENT"
    UNKNOWN = "UNKNOWN"


class HeadcountBracket(str, enum.Enum):
    MICRO = "1-10"
    SMALL = "11-50"
    MEDIUM = "51-200"
    MID_MARKET = "201-500"
    LARGE = "501-1000"
    ENTERPRISE = "1001-5000"
    MEGA = "5000+"
    UNKNOWN = "UNKNOWN"


class CompanyProfile(BaseModel):
    """Output from CompanyVerificationAgent (Tier 2, ParallelAgent sub-agent 4a)."""

    model_config = {"str_strip_whitespace": True}

    company_name: str
    entity_type: EntityType = EntityType.UNKNOWN
    headcount_bracket: HeadcountBracket = HeadcountBracket.UNKNOWN
    funding_stage: FundingStage = FundingStage.UNKNOWN
    founded_year: int | None = Field(None, ge=1800, le=2030)
    hq_location: str | None = None
    linkedin_url: HttpUrl | None = None
    website_url: HttpUrl | None = None
    recent_news_summary: str | None = Field(
        None, max_length=500, description="Last 6 months notable events (layoffs, funding, pivots)"
    )
    confidence: float = Field(..., ge=0.0, le=1.0)
    sources_used: list[str] = Field(default_factory=list)
    verified_at: datetime = Field(default_factory=datetime.utcnow)


class CompanyResearch(BaseModel):
    """Aggregated research output from CompanyResearchAgent (ReAct loop, up to 5 iterations)."""

    model_config = {"str_strip_whitespace": True}

    job_id: str
    company_name: str
    cache_hit: bool = Field(False, description="True if result was served from Firestore cache")
    iterations_used: int = Field(..., ge=1, le=5)
    profile: CompanyProfile
    application_history_summary: str | None = Field(
        None, description="Summary from check_application_history tool result"
    )
    research_summary: str = Field(
        ..., min_length=100, description="Full narrative synthesis of all research"
    )
    key_risks: list[str] = Field(default_factory=list, max_length=10)
    key_positives: list[str] = Field(default_factory=list, max_length=10)
    researched_at: datetime = Field(default_factory=datetime.utcnow)
    cache_ttl_hours: int = Field(
        72, description="How long to cache this research in Firestore (default 72h)"
    )
