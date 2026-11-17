"""Durable initial-embedding publication and processing contracts."""

import hashlib
from enum import Enum

from pydantic import AwareDatetime, BaseModel, Field, model_validator

PROCESSING_VERSION = "initial_embedding_v1"


def publication_id(topic: str, job_id: str) -> str:
    return hashlib.sha256(f"{topic}\n{job_id}\n{PROCESSING_VERSION}".encode()).hexdigest().upper()


class PublicationState(str, Enum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    PUBLISHED = "PUBLISHED"
    UNKNOWN = "UNKNOWN"


class BatchInsertResult(BaseModel):
    inserted_ids: list[str] = Field(default_factory=list)
    existing_ids: list[str] = Field(default_factory=list)
    rejected_ids: list[str] = Field(default_factory=list)
    unconfirmed_ids: list[str] = Field(default_factory=list)
    query_job_id: str | None = None

    @model_validator(mode="after")
    def distinct_outcomes(self):
        groups = [self.inserted_ids, self.existing_ids, self.rejected_ids, self.unconfirmed_ids]
        flattened = [job_id for group in groups for job_id in group]
        if len(flattened) != len(set(flattened)):
            raise ValueError("Insertion outcomes must be disjoint and unique")
        return self


class PublicationClaim(BaseModel):
    publication_id: str = Field(min_length=1)
    job_id: str = Field(min_length=1)
    lease_token: str = Field(min_length=1)
    lease_expires_at: AwareDatetime


class PublicationOwnership(PublicationClaim):
    server_now: AwareDatetime


class CheckpointResult(BaseModel):
    checkpointed_ids: list[str] = Field(default_factory=list)
    lease_lost_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def distinct_ids(self):
        ids = self.checkpointed_ids + self.lease_lost_ids
        if len(ids) != len(set(ids)):
            raise ValueError("Checkpoint outcomes must be unique and disjoint")
        return self


class ProcessingClaim(BaseModel):
    publication_id: str
    job_id: str
    lease_token: str | None = None
    completed: bool = False


class PublishResult(BaseModel):
    claimed_count: int | None = Field(default=None, ge=0, le=100)
    attempted_ids: list[str] = Field(default_factory=list)
    confirmed: dict[str, str] = Field(default_factory=dict)
    unconfirmed_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def valid_outcomes(self):
        attempted = set(self.attempted_ids)
        if self.claimed_count is not None and self.claimed_count < len(attempted):
            raise ValueError("Attempts exceed claims")
        if len(attempted) != len(self.attempted_ids):
            raise ValueError("Duplicate publication attempt")
        if set(self.confirmed) & set(self.unconfirmed_ids):
            raise ValueError("Conflicting publication outcomes")
        if len(self.unconfirmed_ids) != len(set(self.unconfirmed_ids)) or any(
            not v for v in self.confirmed.values()
        ):
            raise ValueError("Invalid publication confirmations")
        if set(self.confirmed) | set(self.unconfirmed_ids) != attempted:
            raise ValueError("Publication outcomes must cover every attempt")
        return self


class QueryOutcomeUnconfirmed(RuntimeError):
    def __init__(self, job_id: str):
        super().__init__("BigQuery query outcome unconfirmed")
        self.job_id = job_id


class QueryFailed(RuntimeError):
    """Confirmed permanent query failure; never automatically retry."""

    def __init__(self, job_id: str, category: str = "query_failed"):
        super().__init__("BigQuery query failed")
        self.job_id = job_id
        self.category = category


class QueryTransactionAborted(QueryFailed):
    """Original terminal query proves rollback; a fresh transaction is safe."""


class ProcessingBusy(RuntimeError):
    """Retry delivery: another worker owns the processing lease."""


class ProcessingLeaseLost(RuntimeError):
    """A stale worker must not commit an embedding."""
