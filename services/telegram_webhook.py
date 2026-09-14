"""services/telegram_webhook.py — Telegram Bot inline-button callback receiver.

Cloud Run service that receives Telegram webhook updates (inline keyboard callbacks).
Delegates to HITLController for action processing (apply/pass/analyze).
Min instances: 1 (always warm for <1s callback response time requirement).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from config.settings import get_settings
from observability.setup import setup_observability
from orchestration.hitl_controller import HITLController
from repositories.actions_repo import ActionsRepository
from repositories.evaluated_repo import EvaluatedRepository

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    setup_observability()
    logger.info("telegram-webhook started")
    yield


app = FastAPI(
    title="telegram-webhook",
    description="Telegram Bot API webhook receiver for HITL inline button callbacks",
    version="1.0.0",
    lifespan=lifespan,
)


def get_hitl_controller() -> HITLController:
    return HITLController(
        actions_repo=ActionsRepository(),
        evaluated_repo=EvaluatedRepository(),
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook/telegram")
async def telegram_update(request: Request) -> JSONResponse:
    """Receive Telegram update (inline keyboard callback_query).

    Expected Telegram update format:
    {
        "update_id": 12345,
        "callback_query": {
            "id": "...",
            "from": {"id": 987654321, ...},
            "data": "apply|job_id_here|trace_id_here",
            "message": {...}
        }
    }
    """
    settings = get_settings()
    try:
        update: dict[str, Any] = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"error": "Invalid JSON"})

    callback_query = update.get("callback_query")
    if not callback_query:
        # Not a callback — could be a regular message, just ack
        return JSONResponse(content={"ok": True})

    callback_data = callback_query.get("data", "")
    from_user = callback_query.get("from", {})
    telegram_user_id = str(from_user.get("id", ""))

    if not callback_data or not telegram_user_id:
        return JSONResponse(content={"ok": True})

    controller = get_hitl_controller()
    result = await controller.handle_callback(
        callback_data=callback_data,
        user_id=telegram_user_id,
    )

    # Answer the callback query to remove the loading spinner
    await _answer_callback_query(
        callback_query_id=callback_query.get("id", ""),
        text=result.get("message", ""),
        token=settings.telegram_bot_token,
    )

    logger.info(
        "Telegram callback handled: user=%s data=%s status=%s",
        telegram_user_id,
        callback_data[:50],
        result.get("status"),
    )
    return JSONResponse(content={"ok": True})


async def _answer_callback_query(
    callback_query_id: str, text: str, token: str
) -> None:
    """Send answerCallbackQuery to dismiss the loading spinner in Telegram."""
    import httpx
    if not token:
        return
    url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(url, json={"callback_query_id": callback_query_id, "text": text[:200]})
        except Exception as exc:  # noqa: BLE001
            logger.warning("answerCallbackQuery failed: %s", exc)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.telegram_webhook:app", host="0.0.0.0", port=8080, workers=1)
