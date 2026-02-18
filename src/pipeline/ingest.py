"""
ingest.py — Fetches job postings from free public APIs and saves them to RAW layer.

Sources used (both free, no API key required):
  - Remotive: https://remotive.com/api/remote-jobs
  - RemoteOK:  https://remoteok.com/api

Key concept — IDEMPOTENCY:
  Running the pipeline twice won't create duplicate rows.
  We check if (source + source_job_id) already exists before inserting.
"""
import json
import datetime as dt

import requests

from .db import connect
from .logger import get_logger

log = get_logger(__name__)

REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
REMOTEOK_URL = "https://remoteok.com/api"


# ── Fetchers ──────────────────────────────────────────────────────────────────

def fetch_remotive() -> list[dict]:
    """Pull jobs from Remotive API. Returns a list of job dicts."""
    log.info("Fetching from Remotive...")
    try:
        response = requests.get(REMOTIVE_URL, timeout=30)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])
        log.info("  → %d jobs fetched from Remotive", len(jobs))
        return jobs
    except Exception as e:
        log.warning("Remotive fetch failed: %s", e)
        return []


def fetch_remoteok() -> list[dict]:
    """Pull jobs from RemoteOK API. Returns a list of job dicts."""
    log.info("Fetching from RemoteOK...")
    try:
        # RemoteOK requires a User-Agent header or it blocks the request
        headers = {"User-Agent": "job-ai-warehouse/1.0"}
        response = requests.get(REMOTEOK_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        # First item is metadata (not a job), filter it out
        jobs = [x for x in data if isinstance(x, dict) and "id" in x and "position" in x]
        log.info("  → %d jobs fetched from RemoteOK", len(jobs))
        return jobs
    except Exception as e:
        log.warning("RemoteOK fetch failed: %s", e)
        return []


# ── Storage ───────────────────────────────────────────────────────────────────

def upsert_raw(source: str, source_job_id: str, payload: dict) -> bool:
    """
    Insert a raw job into the database — but only if it doesn't already exist.
    Returns True if a new row was inserted, False if it was a duplicate.
    """
    con = connect()
    try:
        # Check for existing row (idempotency guard)
        exists = con.execute(
            "SELECT 1 FROM raw_job_postings WHERE source = ? AND source_job_id = ? LIMIT 1",
            [source, source_job_id]
        ).fetchone()

        if exists:
            return False  # Already loaded — skip

        con.execute(
            """
            INSERT INTO raw_job_postings (source, source_job_id, payload_json, ingested_at)
            VALUES (?, ?, ?, ?)
            """,
            [source, source_job_id, json.dumps(payload), dt.datetime.utcnow()]
        )
        return True
    finally:
        con.close()


# ── Main ingest runner ────────────────────────────────────────────────────────

def run_ingest() -> dict:
    """
    Fetch from all sources and load into raw layer.
    Returns a summary dict with counts.
    """
    stats = {"remotive_new": 0, "remoteok_new": 0, "remotive_skipped": 0, "remoteok_skipped": 0}

    for job in fetch_remotive():
        job_id = str(job.get("id", ""))
        if job_id:
            inserted = upsert_raw("remotive", job_id, job)
            if inserted:
                stats["remotive_new"] += 1
            else:
                stats["remotive_skipped"] += 1

    for job in fetch_remoteok():
        job_id = str(job.get("id", ""))
        if job_id:
            inserted = upsert_raw("remoteok", job_id, job)
            if inserted:
                stats["remoteok_new"] += 1
            else:
                stats["remoteok_skipped"] += 1

    log.info(
        "Ingest complete — Remotive: %d new / %d skipped | RemoteOK: %d new / %d skipped",
        stats["remotive_new"], stats["remotive_skipped"],
        stats["remoteok_new"], stats["remoteok_skipped"],
    )
    return stats
