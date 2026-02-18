"""
marts.py — Builds the Gold layer: pre-aggregated analytics tables (marts).

Gold layer is what the dashboard reads. Pre-aggregating here means:
  - Dashboard loads fast (reads a small table, not millions of rows)
  - Analytics logic lives in ONE place (easy to maintain)

Marts built:
  - mart_top_skills_daily     — which skills appear most in job postings
  - mart_skill_by_seniority   — skills broken down by seniority level
  - mart_top_companies        — companies posting the most jobs
"""
from .db import connect
from .logger import get_logger

log = get_logger(__name__)


def build_marts() -> dict:
    """Rebuild all gold mart tables from the silver layer."""
    con = connect()

    # ── mart_top_skills_daily ─────────────────────────────────────────────────
    # Clear and rebuild (simple approach — fine for a portfolio project)
    con.execute("DELETE FROM mart_top_skills_daily;")
    con.execute("""
        INSERT INTO mart_top_skills_daily (date, skill, job_count)
        SELECT
            COALESCE(f.posted_date, CURRENT_DATE) AS date,
            b.skill,
            COUNT(DISTINCT f.job_id)              AS job_count
        FROM fact_job_posting f
        JOIN bridge_job_skill b ON b.job_id = f.job_id
        WHERE b.skill != ''
        GROUP BY 1, 2
        ORDER BY 3 DESC;
    """)

    skill_rows = con.execute("SELECT COUNT(*) FROM mart_top_skills_daily").fetchone()[0]
    log.info("mart_top_skills_daily — %d rows", skill_rows)

    # ── mart_skill_by_seniority ───────────────────────────────────────────────
    con.execute("DELETE FROM mart_skill_by_seniority;")
    con.execute("""
        INSERT INTO mart_skill_by_seniority (seniority, skill, job_count)
        SELECT
            r.seniority,
            b.skill,
            COUNT(DISTINCT f.job_id) AS job_count
        FROM fact_job_posting f
        JOIN dim_role r         ON r.role_id  = f.role_id
        JOIN bridge_job_skill b ON b.job_id   = f.job_id
        WHERE b.skill != ''
          AND r.seniority != 'unknown'
        GROUP BY 1, 2
        ORDER BY 3 DESC;
    """)

    seniority_rows = con.execute("SELECT COUNT(*) FROM mart_skill_by_seniority").fetchone()[0]
    log.info("mart_skill_by_seniority — %d rows", seniority_rows)

    # ── mart_top_companies ────────────────────────────────────────────────────
    con.execute("DELETE FROM mart_top_companies;")
    con.execute("""
        INSERT INTO mart_top_companies (company_name, job_count)
        SELECT
            c.company_name,
            COUNT(DISTINCT f.job_id) AS job_count
        FROM fact_job_posting f
        JOIN dim_company c ON c.company_id = f.company_id
        WHERE c.company_name IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 50;
    """)

    company_rows = con.execute("SELECT COUNT(*) FROM mart_top_companies").fetchone()[0]
    log.info("mart_top_companies — %d rows", company_rows)

    con.close()
    log.info("All marts rebuilt successfully.")
    return {
        "mart_top_skills_daily":   skill_rows,
        "mart_skill_by_seniority": seniority_rows,
        "mart_top_companies":      company_rows,
    }
