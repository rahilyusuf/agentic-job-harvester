"""Lazy, overrideable providers. Importing routers never constructs cloud clients."""

import json
from pathlib import Path
from typing import Protocol

from fastapi import Depends, HTTPException

from devtools.config import resources_configured
from devtools.schemas import GCPStatus, PubSubStatus
from devtools.security import authorize
from schemas.job_models import RawJob

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "Input/dataset_company-career-page-scraper_2026-09-17_06-15-27-909.json"
)


class CloudOperations(Protocol):
    async def gcp_status(self) -> GCPStatus: ...
    async def pubsub_status(self) -> PubSubStatus: ...
    async def insert_once(self, jobs: list[RawJob]) -> list[str]: ...
    async def publish(self, job_ids: list[str]) -> tuple[int, int]: ...


def load_fixture() -> list[object]:
    try:
        items = json.loads(FIXTURE.read_text(encoding="utf-8-sig"))
        if not isinstance(items, list) or len(items) != 215:
            raise ValueError("Unexpected fixture shape")
        return items
    except (OSError, ValueError):
        raise HTTPException(status_code=503, detail="Development fixture unavailable") from None


def cloud_operations(_: None = Depends(authorize)) -> CloudOperations:
    if not resources_configured():
        raise HTTPException(
            status_code=409, detail="GCP configuration is outside the test allowlist"
        )
    from devtools.adapters import GoogleCloudOperations

    return GoogleCloudOperations()
