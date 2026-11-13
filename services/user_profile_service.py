"""services/user_profile_service.py — User profile write path (Streamlit-facing).

Cloud Run service that accepts user profile data (resume + preferences)
and writes it to the user_profiles BigQuery table.
Called by Streamlit frontend → reads secrets from Secret Manager.

Flow:
  Streamlit form → POST /profile → validate → generate embedding → write to BQ
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from config.settings import get_settings
from gateway.client import generate_embedding
from observability.setup import setup_observability
from repositories.profiles_repo import ProfileRepository
from schemas.profile_models import ResumeMetadata, UserProfile

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    setup_observability()
    yield


app = FastAPI(
    title="user-profile-service",
    description="Accepts user profile/resume data and persists to BigQuery user_profiles",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request Models ─────────────────────────────────────────────────────────────

class CreateProfileRequest(BaseModel):
    """Request body for profile creation/update."""

    user_id: str = Field(..., description="Unique user ID (Telegram chat ID or UUID)")
    email: str | None = None
    telegram_chat_id: str | None = None
    resume_text: str = Field(..., min_length=200, description="Full raw resume text")
    resume_gcs_uri: str = Field(..., description="gs://bucket/path/resume.pdf")
    current_title: str
    total_yoe: float = Field(..., ge=0, le=60)
    target_titles: list[str] = Field(..., min_length=1)
    top_skills: list[str] = Field(..., min_length=1)
    preferred_locations: list[str] = Field(default_factory=list)
    open_to_remote: bool = True
    salary_expectation_min_usd: int | None = None
    salary_expectation_max_usd: int | None = None
    notification_threshold_score: int = Field(85, ge=0, le=100)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/profile", status_code=status.HTTP_201_CREATED)
async def create_or_update_profile(request: CreateProfileRequest) -> dict[str, Any]:
    """Create or update a user profile.

    Generates the base resume embedding (e_resume) and sets e_active_user = e_resume
    on first creation. On update, preserves the existing active centroid.
    """
    settings = get_settings()

    # Hash resume text for change detection
    resume_hash = hashlib.sha256(request.resume_text.encode()).hexdigest()

    # Generate base resume embedding
    logger.info("Generating e_resume for user_id=%s", request.user_id)
    embedding_text = (
        f"{request.current_title} {' '.join(request.target_titles)} "
        f"{' '.join(request.top_skills[:20])} {request.resume_text[:2000]}"
    )
    e_resume = await generate_embedding(embedding_text)

    # Check if user already has a profile (preserve active centroid)
    repo = ProfileRepository()
    existing = await repo.get_profile(request.user_id)

    if existing is not None and existing.resume_metadata.resume_text_hash == resume_hash:
        # Resume unchanged — update metadata only, preserve centroid
        e_active = existing.e_active_user
        alpha = existing.active_vector_alpha
        logger.info("Resume unchanged for user_id=%s, preserving centroid", request.user_id)
    else:
        # New or changed resume — reset centroid to base embedding
        e_active = e_resume
        alpha = 0.0
        logger.info("New/updated resume for user_id=%s, resetting centroid", request.user_id)

    resume_metadata = ResumeMetadata(
        total_yoe=request.total_yoe,
        current_title=request.current_title,
        target_titles=request.target_titles,
        top_skills=request.top_skills,
        preferred_locations=request.preferred_locations,
        open_to_remote=request.open_to_remote,
        salary_expectation_min_usd=request.salary_expectation_min_usd,
        salary_expectation_max_usd=request.salary_expectation_max_usd,
        resume_text_hash=resume_hash,
    )

    profile = UserProfile(
        user_id=request.user_id,
        email=request.email,
        telegram_chat_id=request.telegram_chat_id,
        resume_gcs_uri=request.resume_gcs_uri,
        resume_metadata=resume_metadata,
        e_resume=e_resume,
        e_active_user=e_active,
        active_vector_alpha=alpha,
        notification_threshold_score=request.notification_threshold_score,
    )

    await repo.upsert_profile(profile)
    logger.info("Profile upserted for user_id=%s", request.user_id)

    return {
        "user_id": request.user_id,
        "status": "created" if existing is None else "updated",
        "e_resume_dims": len(e_resume),
        "centroid_reset": existing is None or existing.resume_metadata.resume_text_hash != resume_hash,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.user_profile_service:app", host="0.0.0.0", port=8080, workers=1)
