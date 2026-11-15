"""Fixed-fixture preview and explicitly confirmed, bounded ingestion execution."""

from fastapi import APIRouter, Depends, HTTPException

from devtools.dependencies import CloudOperations, cloud_operations, load_fixture
from devtools.schemas import (
    ExecuteRequest,
    ExecuteResponse,
    PreviewResponse,
    SelectedJob,
    SelectionRequest,
)
from services.normalizers.batch import NormalizedBatch, normalize_batch

router = APIRouter(prefix="/ingestion")


def fixture_batch(items: list[object] = Depends(load_fixture)) -> NormalizedBatch:
    return normalize_batch(items)


@router.post("/preview", response_model=PreviewResponse)
async def preview(
    request: SelectionRequest,
    batch: NormalizedBatch = Depends(fixture_batch),
) -> PreviewResponse:
    return PreviewResponse(
        received=batch.received,
        normalized=batch.normalized,
        rejected=batch.rejected,
        unique=len(batch.jobs),
        in_batch_duplicates=batch.in_batch_duplicates,
        selected=[
            SelectedJob(
                job_id=job.job_id,
                title=job.title,
                company=job.company,
                location=job.location,
            )
            for job in batch.jobs[: request.limit]
        ],
    )


@router.post("/execute", response_model=ExecuteResponse)
async def execute(
    request: ExecuteRequest,
    batch: NormalizedBatch = Depends(fixture_batch),
    cloud: CloudOperations = Depends(cloud_operations),
) -> ExecuteResponse:
    jobs = batch.jobs[: request.limit]
    ids = [job.job_id for job in jobs]
    try:
        verified = await cloud.gcp_status()
        if not verified.verified:
            raise ValueError("Unverified resources")
    except Exception:
        raise HTTPException(status_code=409, detail="Test resource verification failed") from None
    try:
        inserted = await cloud.insert_once(jobs)
        if (
            not isinstance(inserted, list)
            or any(not isinstance(job_id, str) for job_id in inserted)
            or len(inserted) != len(set(inserted))
            or not set(inserted).issubset(ids)
        ):
            raise ValueError("Invalid insertion confirmation")
    except Exception:
        raise HTTPException(
            status_code=502,
            detail=(
                "Persistence outcome uncertain; nothing published. "
                "Inspect stored rows before retrying."
            ),
        ) from None
    if not inserted:
        return ExecuteResponse(selected_ids=ids, inserted_ids=[], published=0, publish_failures=0)
    try:
        # Recheck immediately before publishing to the existing embedding topic.
        status = await cloud.pubsub_status()
        if not (status.active and status.pull_only and status.exclusive_subscription):
            raise ValueError("Unsafe subscription")
        published, failures = await cloud.publish(inserted)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Jobs stored; publication unconfirmed. Do not blindly repeat execution.",
        ) from None
    return ExecuteResponse(
        selected_ids=ids,
        inserted_ids=inserted,
        published=published,
        publish_failures=failures,
    )
