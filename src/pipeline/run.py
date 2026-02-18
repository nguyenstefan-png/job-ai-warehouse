"""
run.py — The main entry point. Runs the full pipeline end-to-end.

Pipeline steps (in order):
  1. init_db       — Create tables if they don't exist yet
  2. run_ingest    — Pull job postings from APIs → raw layer
  3. normalize_all — Clean raw JSON → silver tables
  4. run_quality_checks — Validate the data (log warnings if issues)
  5. run_ai_enrichment  — Extract skills + seniority via LLM or fallback
  6. build_marts   — Aggregate silver → gold marts for the dashboard

Run this with:
  python -m src.pipeline.run
"""
import datetime as dt

from .db import init_db
from .ingest import run_ingest
from .normalize import normalize_all
from .quality import run_quality_checks
from .ai_extract import run_ai_enrichment
from .marts import build_marts
from .logger import get_logger

log = get_logger("pipeline.run")


def main():
    start = dt.datetime.utcnow()
    log.info("=" * 60)
    log.info("Pipeline starting at %s", start.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 60)

    # Step 1 — Initialise database
    log.info("STEP 1/6 — Initialising database...")
    init_db()

    # Step 2 — Ingest raw data
    log.info("STEP 2/6 — Ingesting job postings from APIs...")
    ingest_stats = run_ingest()

    # Step 3 — Normalize to silver
    log.info("STEP 3/6 — Normalising raw data to silver tables...")
    norm_stats = normalize_all()

    # Step 4 — Quality checks
    log.info("STEP 4/6 — Running data quality checks...")
    quality_results = run_quality_checks()

    # Step 5 — AI enrichment
    log.info("STEP 5/6 — Running AI skill + seniority extraction...")
    ai_stats = run_ai_enrichment()

    # Step 6 — Build gold marts
    log.info("STEP 6/6 — Building gold analytics marts...")
    mart_stats = build_marts()

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = (dt.datetime.utcnow() - start).total_seconds()
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE in %.1f seconds", elapsed)
    log.info("-" * 60)
    log.info("Ingest   : Remotive %d new | RemoteOK %d new",
             ingest_stats.get("remotive_new", 0), ingest_stats.get("remoteok_new", 0))
    log.info("Normalize: %d inserted, %d skipped (already existed)",
             norm_stats.get("inserted", 0), norm_stats.get("skipped", 0))
    log.info("AI       : %d enriched (%d via LLM, %d via keyword fallback)",
             ai_stats.get("enriched", 0), ai_stats.get("llm", 0), ai_stats.get("fallback", 0))
    quality_pass = sum(1 for v in quality_results.values() if v)
    log.info("Quality  : %d / %d checks passed", quality_pass, len(quality_results))
    log.info("Marts    : %d skill rows, %d seniority rows",
             mart_stats.get("mart_top_skills_daily", 0),
             mart_stats.get("mart_skill_by_seniority", 0))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
