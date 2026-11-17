"""Bounded, lease-aware publication with independently recoverable checkpoints."""

import asyncio
import json
from time import monotonic
from collections.abc import Awaitable, Callable

from google.cloud import pubsub_v1

from repositories.interfaces import IPublicationRepository
from schemas.publication_models import PublishResult

CHECKPOINT_SIZE = 10
PUBLISH_TIMEOUT_SECONDS = 10
LEASE_MARGIN_SECONDS = 30
DISPATCH_TIMEOUT_SECONDS = 900


class _SendNotStarted(RuntimeError):
    """The SDK thread exhausted its lease budget before submitting anything."""


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
    """Publish at most one page, checkpointing every ten actual attempts.

    A lease cannot fence Pub/Sub itself. Acceptance followed by a crash or lost
    checkpoint can still cause duplicate delivery: this is at least once.
    """
    deadline = monotonic() + DISPATCH_TIMEOUT_SECONDS
    async with asyncio.timeout(DISPATCH_TIMEOUT_SECONDS):
        claims = await repository.claim_pending(
            job_ids, limit, min_age_seconds, deadline=deadline,
        )
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
        attempted, confirmed, unconfirmed = [], {}, []
        try:
            for offset in range(0, len(claims), CHECKPOINT_SIZE):
                group = claims[offset:offset + CHECKPOINT_SIZE]
                checked_at = monotonic()
                ownership = await repository.check_ownership(group, deadline=deadline)
                expected = {(c.publication_id, c.job_id, c.lease_token) for c in group}
                actual = [(c.publication_id, c.job_id, c.lease_token) for c in ownership]
                if len(actual) != len(set(actual)) or not set(actual).issubset(expected):
                    raise ValueError("Invalid publication ownership confirmation")
                expires = {
                    c.job_id: min(deadline, checked_at +
                        (c.lease_expires_at - c.server_now).total_seconds())
                    for c in ownership
                }
                group_attempts, broker_confirmed, group_failed = [], {}, []
                for claim in group:
                    send_deadline = expires.get(claim.job_id, 0) - LEASE_MARGIN_SECONDS
                    if monotonic() + PUBLISH_TIMEOUT_SECONDS >= send_deadline:
                        continue  # No send; leave the claim to expire, without claiming success.
                    group_attempts.append(claim)
                    attempted.append(claim.job_id)
                    try:
                        if publish_one is not None:
                            message_id = await asyncio.wait_for(
                                publish_one(claim.job_id), PUBLISH_TIMEOUT_SECONDS,
                            )
                        else:
                            if publisher is None:
                                publisher = await asyncio.wait_for(
                                    asyncio.to_thread(pubsub_v1.PublisherClient),
                                    PUBLISH_TIMEOUT_SECONDS,
                                )
                            path = publisher.topic_path(project_id, topic)

                            def send(job_id=claim.job_id, until=send_deadline):
                                # Recheck INSIDE the thread; executor queuing can consume a lease.
                                if monotonic() + PUBLISH_TIMEOUT_SECONDS >= until:
                                    raise _SendNotStarted("Publication lease budget exhausted")
                                future = publisher.publish(
                                    path, json.dumps({"job_id": job_id}).encode(),
                                    retry=None, timeout=PUBLISH_TIMEOUT_SECONDS,
                                )
                                return future.result(timeout=PUBLISH_TIMEOUT_SECONDS)

                            message_id = await asyncio.wait_for(
                                asyncio.to_thread(send), PUBLISH_TIMEOUT_SECONDS,
                            )
                        if not isinstance(message_id, str) or not message_id:
                            raise ValueError("Missing publication confirmation")
                        broker_confirmed[claim.job_id] = message_id
                    except _SendNotStarted:
                        group_attempts.pop()
                        attempted.pop()
                    except Exception:
                        group_failed.append(claim.job_id)
                if not group_attempts:
                    continue
                # Confirmed transaction aborts retry INSIDE the repository, never the sends.
                checkpoint = await repository.record_outcomes(
                    group_attempts, broker_confirmed, group_failed, deadline=deadline,
                )
                ids = {c.job_id for c in group_attempts}
                if set(checkpoint.checkpointed_ids) | set(checkpoint.lease_lost_ids) != ids:
                    raise ValueError("Incomplete checkpoint confirmation")
                saved = set(checkpoint.checkpointed_ids)
                for claim in group_attempts:
                    if claim.job_id in broker_confirmed and claim.job_id in saved:
                        confirmed[claim.job_id] = broker_confirmed[claim.job_id]
                    else:
                        unconfirmed.append(claim.job_id)
        finally:
            if publisher is not None:
                try:
                    publisher.stop()
                except Exception:
                    pass  # Cleanup must not replace cancellation or known outcomes.
        return PublishResult(
            claimed_count=len(claims), attempted_ids=attempted,
            confirmed=confirmed, unconfirmed_ids=unconfirmed,
        )
