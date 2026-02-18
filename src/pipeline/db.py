"""
db.py — Creates and connects to the DuckDB warehouse.

DuckDB is a file-based database (like SQLite but much faster for analytics).
No server needed — it's just a file: data/jobs.duckdb
"""
import duckdb
from .config import DUCKDB_PATH
from .logger import get_logger

log = get_logger(__name__)


def connect() -> duckdb.DuckDBPyConnection:
    """Open a connection to the warehouse file."""
    return duckdb.connect(DUCKDB_PATH)


def init_db():
    """
    Create all tables if they don't already exist.
    Safe to run multiple times — existing data is never deleted.

    Table layers:
      RAW    — Original JSON payloads from the API (nothing removed)
      SILVER — Cleaned, structured, relational tables
      GOLD   — Pre-aggregated analytics tables (marts)
    """
    log.info("Initialising database at %s", DUCKDB_PATH)
    con = connect()

    # ── RAW LAYER ─────────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_job_postings (
            source         VARCHAR,   -- e.g. 'remotive' or 'remoteok'
            source_job_id  VARCHAR,   -- ID from the source API
            payload_json   VARCHAR,   -- full original JSON as a string
            ingested_at    TIMESTAMP  -- when we pulled this
        );
    """)

    # ── SILVER LAYER — dimension tables ───────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_company (
            company_id   VARCHAR PRIMARY KEY,
            company_name VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id   VARCHAR PRIMARY KEY,
            location_name VARCHAR,
            remote_flag   BOOLEAN
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_role (
            role_id     VARCHAR PRIMARY KEY,
            role_family VARCHAR,  -- e.g. data_engineering, data_science
            seniority   VARCHAR   -- e.g. junior, senior, staff
        );
    """)

    # ── SILVER LAYER — fact table (one row per job posting) ───────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_job_posting (
            job_id           VARCHAR PRIMARY KEY,
            source           VARCHAR,
            source_job_id    VARCHAR,
            title            VARCHAR,
            company_id       VARCHAR,
            location_id      VARCHAR,
            role_id          VARCHAR,
            posted_date      DATE,
            description      VARCHAR,
            description_hash VARCHAR,  -- hash of description for AI cache key
            url              VARCHAR,
            inserted_at      TIMESTAMP
        );
    """)

    # ── SILVER LAYER — skill bridge (many jobs ↔ many skills) ─────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS bridge_job_skill (
            job_id VARCHAR,
            skill  VARCHAR
        );
    """)

    # ── GOLD LAYER — pre-built analytics marts ────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS mart_top_skills_daily (
            date      DATE,
            skill     VARCHAR,
            job_count INTEGER
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mart_skill_by_seniority (
            seniority VARCHAR,
            skill     VARCHAR,
            job_count INTEGER
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mart_top_companies (
            company_name VARCHAR,
            job_count    INTEGER
        );
    """)

    con.close()
    log.info("Database ready — all tables created (or already existed).")
