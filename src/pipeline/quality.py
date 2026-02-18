"""
quality.py — Data quality checks for the Silver layer.

Real data engineers always validate data before it reaches analysts.
These checks catch problems like:
  - Duplicate rows that shouldn't be there
  - NULL values in critical columns
  - Referential integrity issues (a fact row pointing to a non-existent dimension)

If any check fails, the pipeline logs a WARNING (but doesn't crash).
In production you'd fail the run or send an alert — great talking point in interviews.
"""
from .db import connect
from .logger import get_logger

log = get_logger(__name__)


def run_quality_checks() -> dict:
    """
    Run all quality checks. Returns a dict of {check_name: passed (bool)}.
    """
    con = connect()
    results = {}

    # ── Check 1: No duplicate job IDs in fact table ───────────────────────────
    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT job_id, COUNT(*) AS n
            FROM fact_job_posting
            GROUP BY job_id
            HAVING n > 1
        )
    """).fetchone()[0]

    if dupes == 0:
        log.info("✅ PASS — No duplicate job_ids in fact_job_posting")
        results["no_duplicate_jobs"] = True
    else:
        log.warning("❌ FAIL — %d duplicate job_id(s) found in fact_job_posting", dupes)
        results["no_duplicate_jobs"] = False

    # ── Check 2: All fact rows have a valid company_id ────────────────────────
    orphan_companies = con.execute("""
        SELECT COUNT(*)
        FROM fact_job_posting f
        LEFT JOIN dim_company c ON c.company_id = f.company_id
        WHERE c.company_id IS NULL
    """).fetchone()[0]

    if orphan_companies == 0:
        log.info("✅ PASS — All fact rows have a matching dim_company entry")
        results["company_fk_ok"] = True
    else:
        log.warning("❌ FAIL — %d fact rows have no matching company", orphan_companies)
        results["company_fk_ok"] = False

    # ── Check 3: All fact rows have a title (not NULL) ────────────────────────
    null_titles = con.execute("""
        SELECT COUNT(*) FROM fact_job_posting WHERE title IS NULL
    """).fetchone()[0]

    if null_titles == 0:
        log.info("✅ PASS — No NULL titles in fact_job_posting")
        results["no_null_titles"] = True
    else:
        log.warning("❌ FAIL — %d job(s) have NULL title", null_titles)
        results["no_null_titles"] = False

    # ── Check 4: bridge_job_skill has no blank skills ─────────────────────────
    blank_skills = con.execute("""
        SELECT COUNT(*) FROM bridge_job_skill WHERE TRIM(skill) = ''
    """).fetchone()[0]

    if blank_skills == 0:
        log.info("✅ PASS — No blank skill entries in bridge_job_skill")
        results["no_blank_skills"] = True
    else:
        log.warning("❌ FAIL — %d blank skill entries found", blank_skills)
        results["no_blank_skills"] = False

    # ── Check 5: Total job count is reasonable ────────────────────────────────
    total_jobs = con.execute("SELECT COUNT(*) FROM fact_job_posting").fetchone()[0]
    if total_jobs > 0:
        log.info("✅ PASS — fact_job_posting has %d rows (non-empty)", total_jobs)
        results["fact_not_empty"] = True
    else:
        log.warning("❌ FAIL — fact_job_posting is empty!")
        results["fact_not_empty"] = False

    con.close()

    passed = sum(1 for v in results.values() if v)
    total  = len(results)
    log.info("Quality checks: %d / %d passed", passed, total)
    return results
