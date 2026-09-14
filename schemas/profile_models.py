"""schemas/profile_models.py — UserProfile & ResumeMetadata contracts.

UserProfile is written by user_profile_service.py (Streamlit → Secret Mgr → service).
It is the anchor for:
  - e_resume: base embedding from the resume text
  - e_active_user: current centroid used by vector_matcher.py VECTOR_SEARCH
  - ResumeMetadata: structured extraction of resume highlights
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class ResumeMetadata(BaseModel):
    """LLM-extracted structured metadata from a user's resume."""

    model_config = {"str_strip_whitespace": True}

    total_yoe: float = Field(..., ge=0, le=60, description="Total years of relevant experience")
    current_title: str
    target_titles: list[str] = Field(
        ..., min_length=1, max_length=10,
        description="Roles the user is actively targeting",
    )
    top_skills: list[str] = Field(..., min_length=1, max_length=50)
    education_level: str | None = Field(
        None,
        description="Highest degree: PhD | Masters | Bachelors | Associate | Bootcamp | Self-taught",
    )
    industries_worked: list[str] = Field(default_factory=list, max_length=10)
    preferred_locations: list[str] = Field(default_factory=list, max_length=10)
    open_to_remote: bool = True
    salary_expectation_min_usd: int | None = Field(None, ge=0)
    salary_expectation_max_usd: int | None = Field(None, ge=0)
    willing_to_relocate: bool = False
    resume_text_hash: str = Field(
        ..., description="SHA-256 of raw resume text for change detection"
    )


class UserProfile(BaseModel):
    """Full user profile stored in BigQuery user_profiles table."""

    model_config = {"str_strip_whitespace": True}

    user_id: str = Field(..., description="Unique user ID (e.g., Telegram chat ID or UUID)")
    email: str | None = None
    telegram_chat_id: str | None = None
    resume_gcs_uri: str = Field(
        ...,
        description="GCS URI of the raw resume file: gs://bucket/path/resume.pdf",
    )
    resume_metadata: ResumeMetadata
    # Embedding vectors stored as JSON-serialized lists; BigQuery FLOAT64 REPEATED
    e_resume: list[float] = Field(
        ..., min_length=768, max_length=768,
        description="768-dim text-embedding-004 of resume text (base vector)"
    )
    e_active_user: list[float] = Field(
        ..., min_length=768, max_length=768,
        description="768-dim active centroid: (1-α)*e_resume + α*e_accepted_centroid"
    )
    active_vector_alpha: float = Field(
        0.0, ge=0.0, le=1.0,
        description="Current α used in centroid blend; 0.0 = pure resume, 1.0 = pure accepted jobs"
    )
    notification_threshold_score: int = Field(
        85, ge=0, le=100,
        description="Minimum agreed_score to trigger Telegram notification"
    )
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_reflection_at: Optional[datetime] = None
