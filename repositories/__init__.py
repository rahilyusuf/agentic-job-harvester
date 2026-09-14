"""repositories — BigQuery data access layer.

Agents and orchestration code MUST import from interfaces.py (Protocol ABCs).
Never import concrete *_repo.py classes directly outside this package.

Dependency injection example:
    class MyAgent(BaseIntelligenceAgent):
        def __init__(self, repo: IRawJobRepository, ...):
            self._repo = repo
"""

from repositories.interfaces import (
    IActionsRepository,
    IEmbeddingRepository,
    IEvaluatedRepository,
    IProfileRepository,
    IRawJobRepository,
)

__all__ = [
    "IRawJobRepository",
    "IEmbeddingRepository",
    "IEvaluatedRepository",
    "IActionsRepository",
    "IProfileRepository",
]
