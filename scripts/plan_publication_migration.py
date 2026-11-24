"""Offline, evidence-first migration planner. No SDK clients, writes, or queue access.

Run from the repository root with `python -m scripts.plan_publication_migration`.
Inputs are reviewed local exports; stdout is a manifest, NOT an applied migration.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from schemas.publication_models import PROCESSING_VERSION, publication_id


class ConfirmationEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    job_id: str = Field(min_length=1)
    topic: str
    processing_version: Literal["initial_embedding_v1"] = PROCESSING_VERSION
    publication_confirmed: Literal[True]
    evidence_ref: str = Field(min_length=1)
    message_id: str | None = Field(default=None, min_length=1)
    published_at: datetime | None = None

    @field_validator("publication_confirmed", mode="before")
    @classmethod
    def explicit_confirmation(cls, value):
        if value is not True:
            raise ValueError("Explicit confirmation is required")
        return value

    @field_validator("published_at")
    @classmethod
    def explicit_timezone(cls, value):
        if value is not None and value.utcoffset() is None:
            raise ValueError("Historical timestamps require an explicit timezone")
        return value


def build_manifest(
    jobs: list[dict], evidence: list[dict], *, topic: str, expected_count: int = 113
) -> dict:
    ids = [row.get("job_id") for row in jobs]
    if any(not isinstance(job_id, str) or not job_id.strip() for job_id in ids):
        raise ValueError("Export contains invalid job IDs")
    if len(ids) != expected_count or len(set(ids)) != len(ids):
        raise ValueError("Export must contain the exact expected set of distinct jobs")
    if any(row.get("status") != "NEW_RAW" for row in jobs):
        raise ValueError("Review changed source statuses before planning migration")
    confirmations = {}
    for item in evidence:
        proof = ConfirmationEvidence.model_validate(item)
        if proof.topic != topic or proof.job_id not in ids or proof.job_id in confirmations:
            raise ValueError("Evidence topic/job set is inconsistent or duplicated")
        confirmations[proof.job_id] = proof
    rows = []
    for job_id in sorted(ids):
        proof = confirmations.get(job_id)
        rows.append(
            {
                "publication_id": publication_id(topic, job_id),
                "job_id": job_id,
                "topic": topic,
                "processing_version": PROCESSING_VERSION,
                "state": "PUBLISHED" if proof else "UNKNOWN",
                "message_id": proof.message_id if proof else None,
                "published_at": proof.published_at.isoformat()
                if proof and proof.published_at
                else None,
                "evidence_ref": proof.evidence_ref if proof else None,
            }
        )
    return {
        "applied": False,
        "raw_status_changes": 0,
        "automatic_replays": 0,
        "published": len(confirmations),
        "unknown": len(ids) - len(confirmations),
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--expected-count", type=int, default=113)
    args = parser.parse_args()
    try:
        manifest = build_manifest(
            json.loads(args.jobs.read_text(encoding="utf-8-sig")),
            json.loads(args.evidence.read_text(encoding="utf-8-sig")),
            topic=args.topic,
            expected_count=args.expected_count,
        )
    except Exception:
        parser.exit(
            2, "Migration inputs invalid; no operations applied. Review the local exports.\n"
        )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
