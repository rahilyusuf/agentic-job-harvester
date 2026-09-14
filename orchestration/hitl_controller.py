"""orchestration/hitl_controller.py — Telegram HITL state machine.

Manages the pause/resume state for human-in-the-loop interactions.
When a high-score job is detected (agreed_score >= threshold), this controller:
  1. Sends a Telegram notification with inline buttons (✅ Apply | 👎 Pass | 🔍 Analyze)
  2. Records the action via IActionsRepository
  3. Emits a LangFuse user_acceptance score on the original scoring trace
  4. For [🔍 Analyze] — triggers Tier 2 pipeline via Pub/Sub

ARCHITECTURE.md constraint: Never block a Cloud Run thread waiting for HITL.
All state is async and correlation is via job_id + user_id + trace_id.
"""

from __future__ import annotations

import logging

import httpx

from config.settings import get_settings
from observability.setup import score_trace
from repositories.interfaces import IActionsRepository, IEvaluatedRepository
from schemas.job_models import EvaluatedJob

logger = logging.getLogger(__name__)

# Telegram Bot API base
_TG_API_BASE = "https://api.telegram.org/bot{token}"


class HITLController:
    """Telegram HITL state machine for job notification and user action handling.

    Responsible for:
      1. Sending formatted Telegram notifications for high-score jobs
      2. Processing inline button callbacks (apply/pass/analyze)
      3. Routing [🔍 Analyze] actions to Tier 2 pipeline trigger

    Does NOT block waiting for user response — action processing happens in
    telegram_webhook.py when the callback arrives.
    """

    def __init__(
        self,
        actions_repo: IActionsRepository,
        evaluated_repo: IEvaluatedRepository,
    ) -> None:
        self._actions_repo = actions_repo
        self._evaluated_repo = evaluated_repo
        self._settings = get_settings()

    async def notify_high_score_job(
        self,
        evaluation: EvaluatedJob,
        job_title: str,
        company: str,
        job_url: str,
        trace_id: str,
    ) -> bool:
        """Send a Telegram notification for a high-score job.

        Args:
            evaluation: The evaluated job with agreed_score >= threshold.
            job_title: Human-readable job title.
            company: Company name.
            job_url: Direct link to the job posting.
            trace_id: LangFuse trace ID for score correlation.

        Returns:
            True if notification was sent successfully; False otherwise.
        """
        profile = None  # In production, load from user_profiles to get telegram_chat_id
        # For now, use the user_id as the chat_id (configured per user)
        chat_id = evaluation.user_id

        score = evaluation.agreed_score
        veto_flag = " ⚠️ Skeptic concern" if evaluation.skeptic_vetoed else ""

        message = (
            f"🎯 *New Job Match* — Score: {score}/100{veto_flag}\n\n"
            f"*{job_title}*\n"
            f"🏢 {company}\n"
            f"📊 {evaluation.debate_rounds} debate round(s)\n"
            f"✅ Strengths: {', '.join(evaluation.strengths[:3])}\n\n"
            f"[View Job Posting]({job_url})"
        )

        inline_keyboard = {
            "inline_keyboard": [[
                {"text": "✅ Apply", "callback_data": f"apply|{evaluation.job_id}|{trace_id}"},
                {"text": "👎 Pass", "callback_data": f"pass|{evaluation.job_id}|{trace_id}"},
                {"text": "🔍 Analyze", "callback_data": f"analyze|{evaluation.job_id}|{trace_id}"},
            ]]
        }

        success = await self._send_telegram_message(
            chat_id=chat_id,
            text=message,
            reply_markup=inline_keyboard,
        )

        if success:
            await self._evaluated_repo.mark_notified(evaluation.job_id, evaluation.user_id)
            logger.info(
                "Telegram notification sent: job_id=%s user_id=%s score=%d",
                evaluation.job_id,
                evaluation.user_id,
                score,
            )
        return success

    async def handle_callback(
        self,
        callback_data: str,
        user_id: str,
    ) -> dict[str, str]:
        """Process a Telegram inline button callback.

        Args:
            callback_data: The callback_data string from the Telegram update.
                          Format: "action|job_id|trace_id"
            user_id: The Telegram user ID (used as our user_id).

        Returns:
            Dict with "status" and optional "message" keys.
        """
        try:
            parts = callback_data.split("|")
            if len(parts) != 3:
                return {"status": "error", "message": "Invalid callback data format"}

            action, job_id, trace_id = parts

            if action not in ("apply", "pass", "analyze"):
                return {"status": "error", "message": f"Unknown action: {action}"}

            # Record the action in BigQuery
            await self._actions_repo.record_action(
                job_id=job_id,
                user_id=user_id,
                action=action,
                trace_id=trace_id,
            )

            # Emit LangFuse score for HITL telemetry
            score_trace(
                trace_id=trace_id,
                job_id=job_id,
                user_action=action,
            )

            if action == "analyze":
                # Trigger Tier 2 pipeline — publish to Pub/Sub
                await self._trigger_tier2(job_id=job_id, user_id=user_id, trace_id=trace_id)
                return {"status": "ok", "message": "🔍 Analysis started. Check back in ~2 minutes."}

            action_emoji = {"apply": "✅", "pass": "👎"}.get(action, "")
            return {"status": "ok", "message": f"{action_emoji} Got it! Recorded your {action}."}

        except Exception as exc:  # noqa: BLE001
            logger.error("HITL callback error: %s", exc, exc_info=True)
            return {"status": "error", "message": "Internal error processing your action."}

    async def _send_telegram_message(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict | None = None,
    ) -> bool:
        """Send a message via Telegram Bot API."""
        token = self._settings.telegram_bot_token
        if not token:
            logger.warning("Telegram bot token not configured. Skipping notification.")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload: dict = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                return True
            except httpx.HTTPError as exc:
                logger.error("Telegram API error: %s", exc)
                return False

    async def _trigger_tier2(self, job_id: str, user_id: str, trace_id: str) -> None:
        """Publish a Tier 2 trigger message to Pub/Sub."""
        from google.cloud import pubsub_v1
        import json

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            self._settings.gcp_project_id, "tier2-analysis-queue"
        )
        message = json.dumps({
            "job_id": job_id,
            "user_id": user_id,
            "trace_id": trace_id,
        }).encode("utf-8")

        try:
            future = publisher.publish(topic_path, message)
            future.result(timeout=10)
            logger.info("Tier 2 trigger published: job_id=%s user_id=%s", job_id, user_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish Tier 2 trigger: %s", exc)
