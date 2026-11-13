"""schemas/job_models.py — Core job data contracts.

Defines the three stages of a job record lifecycle:
  RawJob              → as ingested from Apify/n8n (raw_job_postings table)
  JobStructuredExtraction → parsed fields extracted by an LLM
  EvaluatedJob        → final scored record written to job_evaluated table
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class JobStatus(str, enum.Enum):
    """State machine for a job record's lifecycle."""

    NEW_RAW = "NEW_RAW"
    EMBEDDING_QUEUED = "EMBEDDING_QUEUED"
    EMBEDDING_DONE = "EMBEDDING_DONE"
    SCORED = "SCORED"
    TIER2_PENDING = "TIER2_PENDING"
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"


class EmploymentType(str, enum.Enum):
    FULL_TIME = "FULL_TIME"
    CONTRACT = "CONTRACT"
    PART_TIME = "PART_TIME"
    FREELANCE = "FREELANCE"
    UNKNOWN = "UNKNOWN"


class RawJob(BaseModel):
    """Represents a job record as ingested — minimal validation, preserves source data."""

    model_config = {"str_strip_whitespace": True}

    job_id: str = Field(..., description="ATS UUID or MD5(job_url)")
    source: str = Field(..., description="Apify actor ID or n8n workflow name")
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    job_url: HttpUrl
    title: str
    company: str
    location: str | None = None
    description: str = Field(..., min_length=50)
    salary_raw: str | None = None
    employment_type_raw: str | None = None
    posted_at: datetime | None = None
    status: JobStatus = JobStatus.NEW_RAW
    ats_platform: str | None = Field(None, description="lever | workable | greenhouse | unknown")

    @field_validator("job_id")
    @classmethod
    def job_id_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("job_id cannot be empty")
        return v.strip()


class JobStructuredExtraction(BaseModel):
    """LLM-extracted structured fields from a RawJob. Used during Tier 1 scoring setup."""

    model_config = {"str_strip_whitespace": True}

    job_id: str
    title_normalized: str = Field(..., description="Cleaned, title-cased role title")
    company_normalized: str
    employment_type: EmploymentType = EmploymentType.UNKNOWN
    location_city: str | None = None
    location_country: str | None = None
    is_remote: bool = False
    yoe_required_min: int | None = Field(None, ge=0, le=40)
    yoe_required_max: int | None = Field(None, ge=0, le=40)
    salary_min_usd: int | None = Field(None, ge=0)
    salary_max_usd: int | None = Field(None, ge=0)
    required_skills: list[str] = Field(default_factory=list, max_length=30)
    preferred_skills: list[str] = Field(default_factory=list, max_length=30)
    is_management_role: bool = False
    requires_clearance: bool = False

    @model_validator(mode="after")
    def validate_yoe_range(self) -> "JobStructuredExtraction":
        if self.yoe_required_min is not None and self.yoe_required_max is not None:
            if self.yoe_required_min > self.yoe_required_max:
                raise ValueError("yoe_required_min must be <= yoe_required_max")
        return self


class EvaluatedJob(BaseModel):
    """Final scored record written to job_evaluated after Tier 1 debate."""

    model_config = {"str_strip_whitespace": True}

    job_id: str
    user_id: str
    scorer_score: int = Field(..., ge=0, le=100, description="ScorerAgent initial score")
    skeptic_score: int = Field(..., ge=0, le=100, description="SkepticAgent agreed score (final)")
    agreed_score: int = Field(..., ge=0, le=100, description="Final score after consensus/veto")
    debate_rounds: int = Field(..., ge=1, le=2)
    skeptic_vetoed: bool = Field(
        False, description="True if Skeptic exercised final veto authority"
    )
    routing_decision: str = Field(..., description="SKIP | PROCESS | HIGH_PRIORITY")
    scorer_rationale: str
    skeptic_rationale: str
    disqualifiers: list[str] = Field(default_factory=list)
    strengths: list[str] = Field(default_factory=list)
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    notified_at: Optional[datetime] = None
    tier2_status: Optional[str] = Field(
        None, description="NULL | PENDING | IN_PROGRESS | COMPLETE"
    )
