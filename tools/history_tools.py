"""tools/history_tools.py — check_application_history tool for CompanyResearchAgent.

Queries BigQuery job_user_actions to see if the user has previously applied to
or passed on a company. This context helps the agent give more personalised advice.

ADK tool function — called by CompanyResearchAgent during its ReAct loop.
"""

from __future__ import annotations

import logging

from google.cloud import bigquery

from config.settings import get_settings

logger = logging.getLogger(__name__)


async def check_application_history(
    company_name: str,
    user_id: str,
    lookback_days: int = 365,
) -> dict[str, object]:
    """Check if the user has previously interacted with this company's jobs.

    Use this during company research to understand the user's history with
    a company — applied before, passed, or analyzed. Helps personalise
    research output and flag repeated opportunities or known bad experiences.

    Args:
        company_name: The company name to search (partial match, case-insensitive).
        user_id: The user to look up history for.
        lookback_days: How far back to look (default 365 days).

    Returns:
        Dict with keys:
            - total_interactions (int): Total actions taken on this company's jobs.
            - applied_count (int): Times user clicked Apply.
            - passed_count (int): Times user clicked Pass.
            - last_action (str | None): Most recent action taken.
            - last_acted_at (str | None): ISO 8601 timestamp of last action.
            - job_ids_seen (list[str]): Job IDs the user has interacted with.
    """
    settings = get_settings()
    client = bigquery.Client(project=settings.gcp_project_id)

    query = f"""
        SELECT
            a.job_id,
            a.action,
            a.acted_at,
            r.company
        FROM `{settings.gcp_project_id}.{settings.bq_dataset}.job_user_actions` a
        INNER JOIN `{settings.gcp_project_id}.{settings.bq_dataset}.raw_job_postings` r
            ON a.job_id = r.job_id
        WHERE a.user_id = '{user_id}'
          AND LOWER(r.company) LIKE '%{company_name.lower().replace("'", "")}%'
          AND a.acted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        ORDER BY a.acted_at DESC
        LIMIT 50
    """

    try:
        rows = list(client.query_and_wait(query))
    except Exception as exc:  # noqa: BLE001
        logger.error("check_application_history failed: %s", exc)
        return {
            "total_interactions": 0,
            "applied_count": 0,
            "passed_count": 0,
            "last_action": None,
            "last_acted_at": None,
            "job_ids_seen": [],
            "error": str(exc),
        }

    applied = [r for r in rows if r.action == "apply"]
    passed = [r for r in rows if r.action == "pass"]
    job_ids = list({r.job_id for r in rows})

    result: dict[str, object] = {
        "total_interactions": len(rows),
        "applied_count": len(applied),
        "passed_count": len(passed),
        "last_action": rows[0].action if rows else None,
        "last_acted_at": rows[0].acted_at.isoformat() if rows else None,
        "job_ids_seen": job_ids[:10],  # cap to avoid bloating agent context
    }

    logger.info(
        "History for company='%s' user='%s': %d total interactions",
        company_name,
        user_id,
        len(rows),
    )
    return result
