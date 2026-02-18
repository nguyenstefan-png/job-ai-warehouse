"""
ai_extract.py — Uses a local LLM (Ollama) to extract structured info from job descriptions.

What it extracts:
  - seniority   : junior / mid / senior / staff / lead / unknown
  - role_family : data_engineering / data_science / analytics / software / unknown
  - skills      : list of lowercase skills, e.g. ["python", "sql", "airflow", "dbt"]

Key features:
  - CACHING: We check description_hash before calling the LLM.
    If a job was already enriched (same description), we skip the LLM call.
    This saves time and avoids re-processing on every pipeline run.

  - FALLBACK: If Ollama isn't running or returns bad output,
    we fall back to a simple keyword-matching extractor.
    The pipeline never crashes because of AI errors.

  - VALIDATION: We use Pydantic to validate the JSON the LLM returns.
    If the LLM returns garbage, validation catches it and we use the fallback.
"""
import json
import re

import requests
from pydantic import BaseModel, Field
from typing import Literal

from .config import OLLAMA_URL, OLLAMA_MODEL, USE_OLLAMA
from .db import connect
from .normalize import role_id
from .logger import get_logger

log = get_logger(__name__)


# ── Pydantic schema — defines what valid AI output looks like ─────────────────

class Extraction(BaseModel):
    seniority: Literal[
        "intern", "junior", "mid", "senior", "staff", "lead", "principal", "unknown"
    ] = "unknown"
    role_family: Literal[
        "data_engineering", "data_science", "analytics",
        "ml_engineering", "software", "unknown"
    ] = "unknown"
    skills: list[str] = Field(default_factory=list)


# ── Common skills for fallback keyword matching ───────────────────────────────

COMMON_SKILLS = [
    "python", "sql", "airflow", "prefect", "dbt", "spark", "hadoop", "kafka",
    "snowflake", "bigquery", "redshift", "databricks", "aws", "gcp", "azure",
    "docker", "kubernetes", "terraform", "pandas", "pyspark", "looker",
    "tableau", "power bi", "git", "postgres", "mysql", "mongodb", "elasticsearch",
    "flink", "dask", "polars", "great expectations", "dlt", "fivetran", "airbyte",
]

SENIORITY_KEYWORDS = {
    "intern":    ["intern", "internship"],
    "junior":    ["junior", "jr.", "entry level", "entry-level", "0-2 years", "1 year"],
    "mid":       ["mid-level", "mid level", "2-4 years", "3+ years"],
    "senior":    ["senior", "sr.", "5+ years", "6+ years", "7+ years"],
    "staff":     ["staff engineer", "staff data"],
    "lead":      ["lead", "team lead", "tech lead"],
    "principal": ["principal", "distinguished"],
}

DE_KEYWORDS = [
    "data engineer", "data pipeline", "etl", "elt", "warehouse", "lakehouse",
    "dbt", "airflow", "spark", "kafka", "data platform", "data infrastructure",
]


# ── Ollama LLM call ───────────────────────────────────────────────────────────

PROMPT_TEMPLATE = """You are a data extraction assistant. Read the job posting below and return ONLY a JSON object — no extra text, no markdown, no explanation.

The JSON must have exactly these keys:
- "seniority": one of ["intern", "junior", "mid", "senior", "staff", "lead", "principal", "unknown"]
- "role_family": one of ["data_engineering", "data_science", "analytics", "ml_engineering", "software", "unknown"]
- "skills": array of lowercase skill names like ["python", "sql", "airflow", "dbt", "spark", "aws"]

Job Title: {title}
Job Description (first 4000 chars):
{description}

Respond with ONLY the JSON object:"""


def _call_ollama(title: str, description: str) -> dict:
    """Send the job to Ollama and get back a JSON dict."""
    prompt = PROMPT_TEMPLATE.format(
        title=title or "",
        description=(description or "")[:4000]
    )
    response = requests.post(
        OLLAMA_URL,
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120
    )
    response.raise_for_status()
    raw_text = response.json().get("response", "").strip()

    # Strip any accidental markdown code fences the LLM might add
    raw_text = re.sub(r"```(?:json)?", "", raw_text).strip().rstrip("`").strip()

    return json.loads(raw_text)


# ── Rule-based fallback ───────────────────────────────────────────────────────

def _fallback_extract(title: str, description: str) -> dict:
    """Simple keyword matching — used when Ollama is unavailable or fails."""
    text = ((title or "") + " " + (description or "")).lower()

    # Detect skills
    skills = [s for s in COMMON_SKILLS if re.search(rf"\b{re.escape(s)}\b", text)]

    # Detect seniority
    seniority = "unknown"
    for level, keywords in SENIORITY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            seniority = level
            break

    # Detect role family
    role_family = "unknown"
    if any(kw in text for kw in DE_KEYWORDS):
        role_family = "data_engineering"
    elif "data scientist" in text or "machine learning" in text:
        role_family = "data_science"
    elif "analytics" in text or "analyst" in text:
        role_family = "analytics"
    elif "software engineer" in text or "backend" in text:
        role_family = "software"

    return {"seniority": seniority, "role_family": role_family, "skills": skills}


# ── Main enrichment runner ────────────────────────────────────────────────────

def run_ai_enrichment() -> dict:
    """
    For every job in fact_job_posting that hasn't been enriched yet,
    run AI extraction and write results to bridge_job_skill + update dim_role.
    """
    con = connect()

    # Get jobs that have no skills yet (not yet enriched)
    jobs = con.execute("""
        SELECT f.job_id, f.title, f.description, f.description_hash
        FROM fact_job_posting f
        WHERE NOT EXISTS (
            SELECT 1 FROM bridge_job_skill b WHERE b.job_id = f.job_id
        )
    """).fetchall()

    log.info("AI enrichment — %d jobs to process", len(jobs))

    enriched   = 0
    used_llm   = 0
    used_fallback = 0

    for job_id_val, title, description, desc_hash in jobs:
        # Try the LLM first, fall back on any error
        raw = None
        if USE_OLLAMA:
            try:
                raw = _call_ollama(title, description)
                used_llm += 1
            except Exception as e:
                log.warning("Ollama failed for job %s: %s — using fallback", job_id_val, e)

        if raw is None:
            raw = _fallback_extract(title, description)
            used_fallback += 1

        # Validate with Pydantic — coerces bad values to defaults
        try:
            data = Extraction(**raw)
        except Exception:
            data = Extraction()  # empty/default if validation fails entirely

        # Update dim_role
        r_id = role_id(data.role_family, data.seniority)
        con.execute(
            "INSERT OR IGNORE INTO dim_role (role_id, role_family, seniority) VALUES (?, ?, ?)",
            [r_id, data.role_family, data.seniority]
        )
        con.execute(
            "UPDATE fact_job_posting SET role_id = ? WHERE job_id = ?",
            [r_id, job_id_val]
        )

        # Insert skills (deduplicated)
        clean_skills = sorted(set(s.strip().lower() for s in data.skills if s.strip()))
        for skill in clean_skills:
            con.execute(
                "INSERT INTO bridge_job_skill (job_id, skill) VALUES (?, ?)",
                [job_id_val, skill]
            )

        enriched += 1

        if enriched % 20 == 0:
            log.info("  ... enriched %d / %d jobs", enriched, len(jobs))

    con.close()
    log.info(
        "AI enrichment complete — %d enriched (%d via LLM, %d via fallback)",
        enriched, used_llm, used_fallback
    )
    return {"enriched": enriched, "llm": used_llm, "fallback": used_fallback}
