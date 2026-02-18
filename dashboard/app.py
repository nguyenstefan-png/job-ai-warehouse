"""
dashboard/app.py — Streamlit dashboard for the Job Market AI Warehouse.

Run with:  streamlit run dashboard/app.py
"""
import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "./data/jobs.duckdb"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Job Market AI Warehouse",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Job Market AI Warehouse")
st.markdown(
    "Real job postings from **Remotive** and **RemoteOK**, "
    "enriched with AI-extracted skills and seniority levels."
)


# ── Database connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


try:
    con = get_connection()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.info("Run the pipeline first:  `python -m src.pipeline.run`")
    st.stop()


# ── Helper ────────────────────────────────────────────────────────────────────
def query(sql: str, params=None) -> pd.DataFrame:
    try:
        if params:
            return con.execute(sql, params).df()
        return con.execute(sql).df()
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()


# ── KPI row ───────────────────────────────────────────────────────────────────
total_jobs     = query("SELECT COUNT(*) AS n FROM fact_job_posting")
total_companies = query("SELECT COUNT(*) AS n FROM dim_company")
total_skills   = query("SELECT COUNT(DISTINCT skill) AS n FROM bridge_job_skill")
enriched_jobs  = query("""
    SELECT COUNT(DISTINCT job_id) AS n FROM bridge_job_skill
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Jobs",      int(total_jobs["n"].iloc[0]) if not total_jobs.empty else 0)
col2.metric("Companies",       int(total_companies["n"].iloc[0]) if not total_companies.empty else 0)
col3.metric("Unique Skills",   int(total_skills["n"].iloc[0]) if not total_skills.empty else 0)
col4.metric("AI-Enriched Jobs", int(enriched_jobs["n"].iloc[0]) if not enriched_jobs.empty else 0)

st.divider()


# ── Top Skills ────────────────────────────────────────────────────────────────
st.header("🔧 Top In-Demand Skills")
st.caption("Skills most frequently mentioned in job postings (AI-extracted)")

top_skills = query("""
    SELECT skill, SUM(job_count) AS total_jobs
    FROM mart_top_skills_daily
    GROUP BY skill
    ORDER BY total_jobs DESC
    LIMIT 25
""")

if not top_skills.empty:
    st.bar_chart(top_skills.set_index("skill")["total_jobs"])
else:
    st.info("No skill data yet. Run the pipeline first.")

st.divider()


# ── Skills by Seniority ───────────────────────────────────────────────────────
st.header("👤 Skills by Seniority Level")
st.caption("Filter by seniority to see what skills are needed at each level")

seniority_options = query("""
    SELECT DISTINCT seniority
    FROM mart_skill_by_seniority
    ORDER BY seniority
""")

if not seniority_options.empty:
    levels = seniority_options["seniority"].tolist()
    selected = st.selectbox("Select seniority level", levels)

    seniority_skills = query("""
        SELECT skill, job_count
        FROM mart_skill_by_seniority
        WHERE seniority = ?
        ORDER BY job_count DESC
        LIMIT 20
    """, [selected])

    if not seniority_skills.empty:
        st.bar_chart(seniority_skills.set_index("skill")["job_count"])
    else:
        st.info("No data for this seniority level.")
else:
    st.info("No seniority data yet. Run the pipeline with Ollama enabled.")

st.divider()


# ── Top Companies ─────────────────────────────────────────────────────────────
st.header("🏢 Most Active Hiring Companies")

top_companies = query("""
    SELECT company_name, job_count
    FROM mart_top_companies
    ORDER BY job_count DESC
    LIMIT 20
""")

if not top_companies.empty:
    st.bar_chart(top_companies.set_index("company_name")["job_count"])
else:
    st.info("No company data yet.")

st.divider()


# ── Latest Jobs Table ─────────────────────────────────────────────────────────
st.header("📋 Latest Job Postings")
st.caption("Most recently ingested jobs with AI-extracted metadata")

latest_jobs = query("""
    SELECT
        f.title,
        c.company_name  AS company,
        l.location_name AS location,
        r.role_family,
        r.seniority,
        f.posted_date,
        f.url
    FROM fact_job_posting f
    JOIN dim_company  c ON c.company_id  = f.company_id
    JOIN dim_location l ON l.location_id = f.location_id
    JOIN dim_role     r ON r.role_id     = f.role_id
    ORDER BY f.inserted_at DESC
    LIMIT 50
""")

if not latest_jobs.empty:
    # Make URLs clickable
    def make_link(row):
        if row["url"]:
            return f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>'
        return row["title"]

    st.dataframe(
        latest_jobs.drop(columns=["url"]),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No jobs yet. Run the pipeline first.")

st.divider()

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    "Built with Python · DuckDB · Ollama · Streamlit | "
    "Data from Remotive & RemoteOK APIs"
)
