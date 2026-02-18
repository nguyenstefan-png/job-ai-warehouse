"""
normalize.py — Reads raw JSON payloads and writes clean, structured Silver tables.

This is the "T" in ELT (Extract → Load raw → Transform).

What we do here:
  - Parse messy fields (titles, company names, locations, dates)
  - Create stable IDs using MD5 hashes (so re-runs don't break FK references)
  - Insert into dim_company, dim_location, dim_role, fact_job_posting
  - Skip rows that already exist (idempotency)
"""
import hashlib
import datetime as dt
import json

from .db import connect
from .logger import get_logger

log = get_logger(__name__)


# ── ID helpers ────────────────────────────────────────────────────────────────
# We use MD5 hashes to create stable, deterministic IDs.
# Same input → same ID every time → safe to re-run without duplicates.

def _md5(text: str) -> str:
    return hashlib.md5((text or "unknown").encode("utf-8")).hexdigest()

def _sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()

def company_id(name: str) -> str:
    return _md5(name or "unknown")

def location_id(name: str) -> str:
    return _md5(name or "unknown")

def role_id(role_family: str, seniority: str) -> str:
    return _md5(f"{role_family}|{seniority}")

def job_id(source: str, source_job_id: str) -> str:
    return _md5(f"{source}:{source_job_id}")


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_date(value) -> dt.date | None:
    """Try to extract a date from various formats (ISO string, Unix timestamp, etc.)."""
    if not value:
        return None
    try:
        if isinstance(value, str):
            return dt.date.fromisoformat(value[:10])
        if isinstance(value, (int, float)):
            return dt.datetime.utcfromtimestamp(value).date()
    except Exception:
        pass
    return None


# ── Per-source field extraction ───────────────────────────────────────────────

def _extract_remotive(payload: dict) -> dict:
    return {
        "title":       payload.get("title"),
        "company":     payload.get("company_name"),
        "location":    payload.get("candidate_required_location") or "Remote",
        "url":         payload.get("url"),
        "description": payload.get("description") or "",
        "posted":      payload.get("publication_date"),
    }

def _extract_remoteok(payload: dict) -> dict:
    return {
        "title":       payload.get("position"),
        "company":     payload.get("company"),
        "location":    payload.get("location") or "Remote",
        "url":         payload.get("url"),
        "description": payload.get("description") or "",
        "posted":      payload.get("date"),
    }


# ── Main normalizer ───────────────────────────────────────────────────────────

def normalize_all() -> dict:
    """
    Read all raw rows and write cleaned records to Silver tables.
    Returns a summary dict.
    """
    con = connect()
    rows = con.execute(
        "SELECT source, source_job_id, payload_json FROM raw_job_postings"
    ).fetchall()

    inserted = 0
    skipped  = 0

    for source, source_job_id, payload_json in rows:
        payload = json.loads(payload_json)

        # Extract fields based on which API this came from
        if source == "remotive":
            fields = _extract_remotive(payload)
        else:
            fields = _extract_remoteok(payload)

        title       = fields["title"]
        company     = fields["company"] or "Unknown"
        location    = fields["location"] or "Remote"
        url         = fields["url"]
        description = fields["description"]
        posted_date = _parse_date(fields["posted"])

        # Generate stable IDs
        c_id = company_id(company)
        l_id = location_id(location)
        r_id = role_id("unknown", "unknown")   # AI step will update this later
        j_id = job_id(source, source_job_id)
        desc_hash = _sha256(description)

        # Skip if this job is already in the fact table
        exists = con.execute(
            "SELECT 1 FROM fact_job_posting WHERE job_id = ? LIMIT 1", [j_id]
        ).fetchone()
        if exists:
            skipped += 1
            continue

        remote_flag = "remote" in location.lower()

        # Upsert dimension tables (INSERT OR IGNORE = safe to re-run)
        con.execute(
            "INSERT OR IGNORE INTO dim_company (company_id, company_name) VALUES (?, ?)",
            [c_id, company]
        )
        con.execute(
            "INSERT OR IGNORE INTO dim_location (location_id, location_name, remote_flag) VALUES (?, ?, ?)",
            [l_id, location, remote_flag]
        )
        con.execute(
            "INSERT OR IGNORE INTO dim_role (role_id, role_family, seniority) VALUES (?, ?, ?)",
            [r_id, "unknown", "unknown"]
        )

        # Insert fact row
        con.execute(
            """
            INSERT INTO fact_job_posting
                (job_id, source, source_job_id, title, company_id, location_id, role_id,
                 posted_date, description, description_hash, url, inserted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """,
            [j_id, source, source_job_id, title, c_id, l_id, r_id,
             posted_date, description, desc_hash, url]
        )
        inserted += 1

    con.close()
    log.info("Normalize complete — %d inserted, %d already existed", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}
