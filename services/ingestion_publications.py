"""Compose atomic persistence and claimed publication for the opt-in ingestion path."""

import logging

from fastapi import HTTPException

from config.settings import Settings
from observability.ingestion_logging import context, emit, error_fields, stage
from repositories.raw_jobs_repo import RawJobRepository
from services.normalizers.batch import NormalizedBatch
from services.publication_dispatcher import dispatch_pending


async def ingest_with_publications(
    batch: NormalizedBatch,
    *,
    dataset_id: str,
    actor_run_id: str,
    settings: Settings,
    repository=None,
    dispatcher=dispatch_pending,
) -> dict:
    stage("bigquery")
    emit("ingestion.bigquery.started", unique=len(batch.jobs))
    repository = repository if repository is not None else RawJobRepository(settings=settings)
    try:
        persisted = await repository.persist_jobs(batch.jobs)
        expected = {job.job_id for job in batch.jobs}
        if (
            persisted.unconfirmed_ids
            or persisted.rejected_ids
            or set(persisted.inserted_ids) | set(persisted.existing_ids) != expected
        ):
            raise ValueError("Persistence not confirmed")
    except Exception as exc:
        raise HTTPException(502, "BigQuery insert failed; see service logs") from exc
    inserted = set(persisted.inserted_ids)
    emit(
        "ingestion.bigquery.completed",
        inserted=len(inserted),
        unique=len(expected),
        existing_duplicates=len(persisted.existing_ids),
    )
    recovered = published = failures = 0
    # Each full page removes successful IDs from the eligible set. A maximum of
    # ceil(batch_size/100) pages bounds the request, without the old 100-job starvation.
    for _ in range((len(expected) + 99) // 100):
        stage("recovery")
        emit("ingestion.recovery.started")

        def claimed(claims):
            nonlocal recovered
            recovered += sum(claim.job_id not in inserted for claim in claims)
            emit("ingestion.recovery.completed", recovered=recovered, outcome="success")
            stage("pubsub")
            emit("ingestion.pubsub.started")

        try:
            result = await dispatcher(
                repository.publications,
                project_id=settings.gcp_project_id,
                topic=settings.pubsub_embedding_topic,
                job_ids=list(expected),
                on_claimed=claimed,
            )
        except Exception as exc:
            emit(
                "ingestion.publication.unconfirmed",
                logging.WARNING,
                error_category="publication_unconfirmed",
                **error_fields(exc),
            )
            raise HTTPException(502, "Jobs stored; publication state unconfirmed") from exc
        claimed_count = (
            result.claimed_count if result.claimed_count is not None else len(result.attempted_ids)
        )
        if not claimed_count:
            emit("ingestion.recovery.completed", recovered=recovered, outcome="success")
            break
        published += len(result.confirmed)
        failures += len(result.unconfirmed_ids)
        emit(
            "ingestion.pubsub.completed",
            published=published,
            publish_failures=failures,
            outcome="unconfirmed"
            if failures and not published
            else "partial"
            if failures
            else "success",
            subscriber_processing="not_observed",
        )
        if claimed_count < 100:
            break
    try:
        publications = repository.publications
        if publications is None:
            raise ValueError("Publication ledger is not configured")
        counts = await publications.publication_counts(list(expected))
    except Exception as exc:
        raise HTTPException(502, "Jobs stored; publication state unconfirmed") from exc
    if any(counts.get(state, 0) for state in ("PENDING", "IN_FLIGHT", "UNKNOWN")):
        current = context()
        if current:
            current.incomplete = True
        emit(
            "ingestion.publication.deferred",
            logging.WARNING,
            reason="publication_pending_or_held",
            publication_pending=counts.get("PENDING", 0),
            publication_in_flight=counts.get("IN_FLIGHT", 0),
            publication_unknown=counts.get("UNKNOWN", 0),
        )
    return {
        "dataset_id": dataset_id,
        "actor_run_id": actor_run_id,
        "received": batch.received,
        "normalized": batch.normalized,
        "rejected": batch.rejected,
        "unique": len(batch.jobs),
        "in_batch_duplicates": batch.in_batch_duplicates,
        "apply_url_fallback": batch.apply_url_fallback,
        "inserted": len(inserted),
        "recovered": recovered,
        "published": published,
        "publish_failures": failures,
    }
