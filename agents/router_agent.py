"""agents/router_agent.py — JobIntakeRouter.

Implements ARCHITECTURE.md Step D (Tier 1):
    Reads Firestore dynamic_preference_rules, returns SKIP | PROCESS | HIGH_PRIORITY.
    On SKIP, halt immediately (~$0.0001 cost, no further agents run).

Model: "router-flash" (cheapest viable model — min context, max 512 tokens output)
"""

from __future__ import annotations

import json
import logging

from google.cloud import firestore

from agents.base import BaseIntelligenceAgent
from config.settings import get_settings
from observability.setup import observe
from schemas.job_models import RawJob
from schemas.routing_models import RouterDecision

logger = logging.getLogger(__name__)

_PREFERENCE_RULES_COLLECTION = "config_cache"
_PREFERENCE_RULES_DOC = "dynamic_preference_rules"


class JobIntakeRouter(BaseIntelligenceAgent):
    """Tier 1 Step D: Route a job to SKIP | PROCESS | HIGH_PRIORITY.

    Reads the user's Firestore dynamic_preference_rules written by ReflectionAgent.
    Returns a RouterDecision. SKIP terminates the pipeline immediately.

    Usage:
        router = JobIntakeRouter()
        decision = await router.route(job, user_id="user_123")
        if decision.decision == RoutingDecision.SKIP:
            return  # halt pipeline
    """

    MODEL_ALIAS = "router-flash"
    OUTPUT_SCHEMA = RouterDecision

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._settings = get_settings()
        self._fs = firestore.AsyncClient(
            project=self._settings.gcp_project_id,
            database=self._settings.firestore_database_id,
        )

    def _system_prompt(self) -> str:
        return (
            "You are a job intake router for a Principal AI Engineer. "
            "Your task is to evaluate whether a job posting is worth scoring in depth. "
            "You have access to the user's dynamic preference rules. "
            "Be highly decisive: if there is any clear mismatch, return SKIP immediately. "
            "Reserve HIGH_PRIORITY for roles that are an exceptionally strong match. "
            "Return only SKIP, PROCESS, or HIGH_PRIORITY — never ask for clarification."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        preference_rules: list[dict],
        user_context: str,
    ) -> list[dict[str, str]]:
        rules_text = json.dumps(preference_rules, indent=2) if preference_rules else "No rules yet."
        return [
            self._system_message(),
            self._user_message(
                f"## User Context\n{user_context}\n\n"
                f"## Dynamic Preference Rules\n{rules_text}\n\n"
                f"## Job Posting\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Location: {job.location or 'Not specified'}\n"
                f"Employment: {job.employment_type_raw or 'Not specified'}\n"
                f"Description (first 1500 chars):\n{job.description[:1500]}\n\n"
                f"Make a routing decision. job_id={job.job_id}"
            ),
        ]

    @observe(name="JobIntakeRouter")
    async def route(self, job: RawJob, user_id: str) -> RouterDecision:
        """Route a job posting for a specific user.

        Args:
            job: The RawJob to evaluate.
            user_id: The user to route for (used to fetch their preference rules).

        Returns:
            RouterDecision with SKIP | PROCESS | HIGH_PRIORITY decision.
        """
        preference_rules = await self._load_preference_rules(user_id)
        user_context = f"user_id={user_id}"

        messages = self._build_messages(job, preference_rules, user_context)
        decision: RouterDecision = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=self._new_trace_id(),
        )
        logger.info(
            "Router decision: job_id=%s decision=%s confidence=%.2f",
            job.job_id,
            decision.decision,
            decision.confidence,
        )
        return decision

    async def _load_preference_rules(self, user_id: str) -> list[dict]:
        """Load dynamic_preference_rules from Firestore for the given user."""
        try:
            doc_ref = (
                self._fs.collection(_PREFERENCE_RULES_COLLECTION)
                .document(user_id)
            )
            doc = await doc_ref.get()
            if doc.exists:
                data = doc.to_dict() or {}
                return data.get("rules", [])
            return []
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load preference rules for user_id=%s: %s", user_id, exc)
            return []
