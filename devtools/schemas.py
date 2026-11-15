"""Transport schemas; no cloud clients or credential-bearing response models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt


class SelectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: StrictInt = Field(default=3, ge=1, le=3)


class ExecuteRequest(SelectionRequest):
    confirmation: Literal["WRITE_THREE_OR_FEWER_TEST_JOBS"]


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    environment: Literal["development"] = "development"
    cloud_writes_automatic: Literal[False] = False


class SelectedJob(BaseModel):
    job_id: str
    title: str
    company: str
    location: str | None


class PreviewResponse(BaseModel):
    received: int
    normalized: int
    rejected: int
    unique: int
    in_batch_duplicates: int
    selected: list[SelectedJob]


class PubSubStatus(BaseModel):
    topic: str
    subscription: str
    active: bool
    pull_only: bool
    exclusive_subscription: bool
    ack_deadline_seconds: int
    retention_seconds: int


class GCPStatus(BaseModel):
    project: str
    dataset: str
    location: str
    table: str
    verified: bool
    pubsub: PubSubStatus


class ExecuteResponse(BaseModel):
    selected_ids: list[str]
    inserted_ids: list[str]
    published: int
    publish_failures: int
