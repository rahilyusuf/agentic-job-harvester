"""agents/reflection_agent.py — ReflectionAgent (Weekly active learning flywheel).

Implements ARCHITECTURE.md Section 4:
  - Queries 30 days of HITL telemetry + LangFuse traces
  - Synthesises rules into Firestore dynamic_preference_rules
  - Recalculates active vector centroid:
      e_active_new = (1 - α) * e_resume + α * e_accepted_centroid

Model: "deep-pro" (weekly, cost-tolerant — this run happens once per week)
"""

from __future__ import annotations

import logging
import numpy as np
from datetime import timezone

from google.cloud import firestore

from agents.base import BaseIntelligenceAgent
from config.settings import get_settings
from observability.setup import observe
from repositories.interfaces import IActionsRepository, IEmbeddingRepository, IProfileRepository
from schemas.reflection_models import PreferenceRules

logger = logging.getLogger(__name__)

_PREFERENCE_RULES_COLLECTION = "config_cache"
_DEFAULT_ALPHA = 0.3  # centroid drift rate


class ReflectionAgent(BaseIntelligenceAgent):
    """Weekly active learning agent that synthesises HITL signals into preference rules.

    Workflow:
      1. Load 30-day accepted + passed job IDs from job_user_actions.
      2. Fetch accepted job embeddings → compute centroid.
      3. Recalculate e_active_user = (1-α)*e_resume + α*e_accepted_centroid.
      4. LLM synthesises pattern → Firestore preference rules.
      5. Write updated centroid to user_profiles.
      6. Write rules to Firestore config_cache.

    Usage (called by reflection_runner.py):
        agent = ReflectionAgent(actions_repo=..., embedding_repo=..., profile_repo=...)
        rules = await agent.reflect(user_id="user_123")
    """

    MODEL_ALIAS = "deep-pro"
    OUTPUT_SCHEMA = PreferenceRules

    def __init__(
        self,
        actions_repo: IActionsRepository,
        embedding_repo: IEmbeddingRepository,
        profile_repo: IProfileRepository,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._actions_repo = actions_repo
        self._embedding_repo = embedding_repo
        self._profile_repo = profile_repo
        self._settings = get_settings()
        self._fs = firestore.AsyncClient(
            project=self._settings.gcp_project_id,
            database=self._settings.firestore_database_id,
        )

    def _system_prompt(self) -> str:
        return (
            "You are a career preference learning engine. "
            "Analyse a candidate's 30-day job interaction history (applied vs passed). "
            "Synthesise 3-10 specific, actionable preference rules that explain their patterns. "
            "Rules must be concrete — not 'prefers startups' but 'VC-backed Series A/B startups "
            "in AI/ML space with <100 employees'. "
            "Assign score_delta for BOOST/PENALIZE rules (-50 to +50). "
            "Assign confidence based on signal count (more interactions = higher confidence)."
        )

    def _build_messages(  # type: ignore[override]
        self,
        user_id: str,
        applied_summaries: list[str],
        passed_summaries: list[str],
        existing_rules: list[dict],
    ) -> list[dict[str, str]]:
        applied_text = "\n".join(f"- {s}" for s in applied_summaries[:20]) or "None"
        passed_text = "\n".join(f"- {s}" for s in passed_summaries[:20]) or "None"
        existing_text = "\n".join(
            f"- {r.get('condition', '')} ({r.get('action', '')})"
            for r in existing_rules[:10]
        ) or "No existing rules."

        return [
            self._system_message(),
            self._user_message(
                f"## User: {user_id}\n"
                f"## Applied To (last 30 days)\n{applied_text}\n\n"
                f"## Passed On (last 30 days)\n{passed_text}\n\n"
                f"## Existing Rules (to refine or replace)\n{existing_text}\n\n"
                "Synthesise updated PreferenceRules. "
                "Be specific. Base rules only on patterns evident in the data."
            ),
        ]

    @observe(name="ReflectionAgent")
    async def reflect(self, user_id: str) -> PreferenceRules:
        """Run the full reflection cycle for a user.

        Args:
            user_id: The user to reflect for.

        Returns:
            Updated PreferenceRules written to Firestore.
        """
        trace_id = self._new_trace_id()

        # Step 1: Load telemetry
        accepted_ids = await self._actions_repo.get_accepted_jobs(user_id, since_days=30)
        action_counts = await self._actions_repo.get_action_counts(user_id, since_days=30)
        profile = await self._profile_repo.get_profile(user_id)

        if profile is None:
            raise ValueError(f"No profile found for user_id={user_id!r}")

        # Step 2: Compute accepted jobs centroid
        centroid_updated = False
        new_centroid = profile.e_active_user
        new_alpha = profile.active_vector_alpha

        if accepted_ids:
            accepted_embeddings = []
            for job_id in accepted_ids:
                emb = await self._embedding_repo.get_embedding(job_id)
                if emb:
                    accepted_embeddings.append(emb)

            if accepted_embeddings:
                e_accepted = np.mean(accepted_embeddings, axis=0).tolist()
                alpha = min(new_alpha + 0.05, _DEFAULT_ALPHA)  # drift slowly toward accepted
                e_resume = profile.e_resume
                new_centroid = [
                    (1 - alpha) * r + alpha * a
                    for r, a in zip(e_resume, e_accepted)
                ]
                new_alpha = alpha
                centroid_updated = True

        # Step 3: Load existing rules
        existing_rules = await self._load_existing_rules(user_id)

        # Step 4: Synthesise new rules via LLM
        messages = self._build_messages(
            user_id=user_id,
            applied_summaries=[f"job_id={jid}" for jid in accepted_ids],
            passed_summaries=[],  # Would join with raw_job_postings in production
            existing_rules=existing_rules,
        )
        rules: PreferenceRules = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=trace_id,
        )

        # Override metadata fields
        object.__setattr__(rules, "user_id", user_id)
        object.__setattr__(rules, "centroid_updated", centroid_updated)
        object.__setattr__(rules, "alpha_used", new_alpha)
        object.__setattr__(rules, "accepted_jobs_count", action_counts.get("apply", 0))
        object.__setattr__(rules, "passed_jobs_count", action_counts.get("pass", 0))
        object.__setattr__(rules, "reflection_trace_id", trace_id)

        # Step 5: Persist centroid
        if centroid_updated:
            await self._profile_repo.update_active_centroid(
                user_id=user_id,
                new_centroid=new_centroid,
                alpha=new_alpha,
            )

        # Step 6: Write rules to Firestore
        await self._write_rules(user_id, rules)

        logger.info(
            "ReflectionAgent: user_id=%s rules=%d centroid_updated=%s alpha=%.3f",
            user_id,
            len(rules.rules),
            centroid_updated,
            new_alpha,
        )
        return rules

    async def _load_existing_rules(self, user_id: str) -> list[dict]:
        try:
            doc = await (
                self._fs.collection(_PREFERENCE_RULES_COLLECTION)
                .document(user_id)
                .get()
            )
            if doc.exists:
                return (doc.to_dict() or {}).get("rules", [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load existing rules: %s", exc)
        return []

    async def _write_rules(self, user_id: str, rules: PreferenceRules) -> None:
        data = rules.model_dump(mode="json")
        await (
            self._fs.collection(_PREFERENCE_RULES_COLLECTION)
            .document(user_id)
            .set(data, merge=False)
        )
        logger.info("Wrote %d preference rules for user_id=%s", len(rules.rules), user_id)
