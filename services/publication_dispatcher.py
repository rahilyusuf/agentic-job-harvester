"""Claim-first publication. Broker confirmation and worker completion are separate."""

import asyncio
import json
from collections.abc import Awaitable, Callable

from google.cloud import pubsub_v1

from repositories.interfaces import IPublicationRepository
from schemas.publication_models import PublishResult


async def dispatch_pending(
    repository: IPublicationRepository,
    *,
    project_id: str,
    topic: str,
    job_ids: list[str] | None = None,
    limit: int = 100,
    min_age_seconds: int = 0,
    publish_one: Callable[[str], Awaitable[str]] | None = None,
    on_claimed: Callable[[list], None] | None = None,
) -> PublishResult:
    """One bounded page. Unconfirmed publications wait until their original lease expires.

    A cancellation/crash may leave acknowledged messages IN_FLIGHT. Expired claims
    recover these at least once; no distributed exactly-once guarantee is possible.
    """
    claims = await repository.claim_pending(job_ids, limit, min_age_seconds)
    if not claims:
        return PublishResult()
    if (
        len({c.job_id for c in claims}) != len(claims)
        or len({c.publication_id for c in claims}) != len(claims)
        or len(claims) > limit
        or (job_ids is not None and not {c.job_id for c in claims}.issubset(job_ids))
    ):
        raise ValueError("Invalid publication claim set")
    if on_claimed is not None:
        on_claimed(claims)
    publisher = None
    confirmed, unconfirmed = {}, []
    try:
        if publish_one is None:
            publisher = pubsub_v1.PublisherClient()
            path = publisher.topic_path(project_id, topic)

            async def send(job_id: str) -> str:
                def publish():
                    future = publisher.publish(path, json.dumps({"job_id": job_id}).encode())
                    return future.result(timeout=10)

                return await asyncio.to_thread(publish)

            publish_one = send
        for claim in claims:
            try:
                message_id = await publish_one(claim.job_id)
                if not isinstance(message_id, str) or not message_id:
                    raise ValueError("Missing publication confirmation")
                confirmed[claim.job_id] = message_id
            except Exception:
                unconfirmed.append(claim.job_id)
    except Exception:
        unconfirmed = [c.job_id for c in claims if c.job_id not in confirmed]
    finally:
        if publisher is not None:
            try:
                publisher.stop()
            except Exception:
                pass  # Cleanup must not replace cancellation or known outcomes.
    # If this update fails, do not report durable publication success. Leases remain recoverable.
    await repository.record_outcomes(claims, confirmed, unconfirmed)
    return PublishResult(
        attempted_ids=[c.job_id for c in claims],
        confirmed=confirmed,
        unconfirmed_ids=unconfirmed,
    )
