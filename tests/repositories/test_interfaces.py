"""tests/repositories/test_interfaces.py — Protocol compliance tests.

Verifies that concrete repo classes satisfy their Protocol interfaces.
No live BigQuery calls — uses duck-typing Protocol checks.
"""

from __future__ import annotations

import pytest

from repositories.interfaces import (
    IActionsRepository,
    IEmbeddingRepository,
    IEvaluatedRepository,
    IProfileRepository,
    IRawJobRepository,
)
from repositories.raw_jobs_repo import RawJobRepository
from repositories.embeddings_repo import EmbeddingRepository
from repositories.evaluated_repo import EvaluatedRepository
from repositories.actions_repo import ActionsRepository
from repositories.profiles_repo import ProfileRepository
from unittest.mock import MagicMock

from schemas.job_models import RawJob


def make_mock_bq_client() -> MagicMock:
    """Return a mock BigQuery client to avoid real GCP calls."""
    client = MagicMock()
    client.query_and_wait = MagicMock(return_value=[])
    client.insert_rows_json = MagicMock(return_value=[])
    return client


@pytest.fixture
def mock_bq() -> MagicMock:
    return make_mock_bq_client()


@pytest.mark.unit
def test_raw_jobs_repo_satisfies_protocol(mock_bq: MagicMock) -> None:
    """RawJobRepository must satisfy IRawJobRepository Protocol."""
    repo = RawJobRepository(bq_client=mock_bq, dataset="test_dataset")
    assert isinstance(repo, IRawJobRepository), (
        "RawJobRepository does not satisfy IRawJobRepository protocol. "
        "Check that all method signatures match."
    )


@pytest.mark.unit
def test_embedding_repo_satisfies_protocol(mock_bq: MagicMock) -> None:
    """EmbeddingRepository must satisfy IEmbeddingRepository Protocol."""
    repo = EmbeddingRepository(bq_client=mock_bq, dataset="test_dataset")
    assert isinstance(repo, IEmbeddingRepository)


@pytest.mark.unit
def test_evaluated_repo_satisfies_protocol(mock_bq: MagicMock) -> None:
    """EvaluatedRepository must satisfy IEvaluatedRepository Protocol."""
    repo = EvaluatedRepository(bq_client=mock_bq, dataset="test_dataset")
    assert isinstance(repo, IEvaluatedRepository)


@pytest.mark.unit
def test_actions_repo_satisfies_protocol(mock_bq: MagicMock) -> None:
    """ActionsRepository must satisfy IActionsRepository Protocol."""
    repo = ActionsRepository(bq_client=mock_bq, dataset="test_dataset")
    assert isinstance(repo, IActionsRepository)


@pytest.mark.unit
def test_profile_repo_satisfies_protocol(mock_bq: MagicMock) -> None:
    """ProfileRepository must satisfy IProfileRepository Protocol."""
    repo = ProfileRepository(bq_client=mock_bq, dataset="test_dataset")
    assert isinstance(repo, IProfileRepository)


@pytest.mark.unit
def test_invalid_actions_repo_action_raises() -> None:
    """ActionsRepository.record_action must reject invalid action strings."""
    import asyncio
    repo = ActionsRepository(bq_client=make_mock_bq_client(), dataset="test_dataset")

    with pytest.raises(ValueError, match="Invalid action"):
        asyncio.run(repo.record_action(
            job_id="test", user_id="user", action="unknown_action"
        ))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raw_jobs_batch_insert_uses_array_parameter(mock_bq: MagicMock) -> None:
    """batch_insert_jobs must bind job_ids via bigquery.ArrayQueryParameter."""
    repo = RawJobRepository(bq_client=mock_bq, dataset="test_dataset")
    job = RawJob(
        job_id="test_id_123",
        job_url="https://example.com/jobs/123",  # type: ignore[arg-type]
        title="AI Engineer",
        company="TechCorp",
        location="Dubai",
        description="Build LLM applications with python and langchain for enterprise.",
        source="test",
    )
    inserted = await repo.batch_insert_jobs([job])
    assert inserted == ["test_id_123"]
    mock_bq.query_and_wait.assert_called_once()
    call_args, call_kwargs = mock_bq.query_and_wait.call_args
    assert "UNNEST(@ids)" in call_args[0]
    job_config = call_kwargs.get("job_config")
    assert job_config is not None
    assert len(job_config.query_parameters) == 1
    param = job_config.query_parameters[0]
    assert param.name == "ids"
    assert param.values == ["test_id_123"]

