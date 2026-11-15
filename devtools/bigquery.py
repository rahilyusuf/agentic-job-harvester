"""Read-only GCP metadata endpoint; no general SQL or administration interface."""

from fastapi import APIRouter, Depends, HTTPException

from devtools.dependencies import CloudOperations, cloud_operations
from devtools.schemas import GCPStatus

router = APIRouter()


@router.get("/gcp/status", response_model=GCPStatus)
async def status(cloud: CloudOperations = Depends(cloud_operations)) -> GCPStatus:
    try:
        return await cloud.gcp_status()
    except Exception:
        raise HTTPException(status_code=503, detail="Test resource verification failed") from None
