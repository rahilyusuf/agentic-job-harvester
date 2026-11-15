"""Central development router. Only the gated local app imports this module."""

from fastapi import APIRouter, Depends

from devtools import bigquery, ingestion, pubsub
from devtools.schemas import HealthResponse
from devtools.security import authorize

router = APIRouter(prefix="/dev", tags=["development"], dependencies=[Depends(authorize)])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse()


router.include_router(bigquery.router)
router.include_router(ingestion.router)
router.include_router(pubsub.router)
# Embedding execution deliberately has no router or import until separately authorized.
