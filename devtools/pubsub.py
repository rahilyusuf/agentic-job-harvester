"""Read-only subscription verification; never pulls or acknowledges messages."""

from fastapi import APIRouter, Depends, HTTPException

from devtools.dependencies import CloudOperations, cloud_operations
from devtools.schemas import PubSubStatus

router = APIRouter()


@router.get("/pubsub/status", response_model=PubSubStatus)
async def status(cloud: CloudOperations = Depends(cloud_operations)) -> PubSubStatus:
    try:
        return await cloud.pubsub_status()
    except Exception:
        raise HTTPException(
            status_code=503, detail="Test subscription verification failed"
        ) from None
