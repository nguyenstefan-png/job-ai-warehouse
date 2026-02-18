"""
dashboard/app.py — Streamlit dashboard reading from CSV exports.
Works both locally and on Streamlit Cloud.
"""
import pandas as pd
import streamlit as st
from pathlib import Path

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

# ── Load CSV data ─────────────────────────────────────────────────────────────
ASSETS = Path(__file__).parent.parent / "assets"
st.write(str(ASSETS))  # temporary debug line

@st.cache_data
def load_data():
    try:
        skills     = pd.read_csv(ASSETS / "skills.csv")
        seniority  = pd.read_csv(ASSETS / "seniority.csv")
        companies  = pd.read_csv(ASSETS / "companies.csv")
        jobs       = pd.read_csv(ASSETS / "jobs.csv")
        return skills, seniority, companies, jobs
    except FileNotFoundError as e:
        return None, None, None, None

skills_df, seniority_df, companies_df, jobs_df = load_data()

if skills_df is None:
    st.error("Data files not found. Run the export script locally and push the assets/ folder to GitHub.")
    st.code("python -c \"import duckdb; con = duckdb.connect('./data/jobs.duckdb'); con.execute('SELECT * FROM mart_top_skills_daily').df().to_csv('assets/skills.csv', index=False); con.close()\"")
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
total_jobs      = len(jobs_df)
total_companies = companies_df["company_name"].nunique() if companies_df is not None else 0
total_skills    = skills_df["skill"].nunique() if skills_df is not None else 0
enriched        = len(jobs_df[jobs_df["role_family"] != "unknown"]) if jobs_df is not None else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Jobs",       total_jobs)
col2.metric("Companies",        total_companies)
col3.metric("Unique Skills",    total_skills)
col4.metric("AI-Enriched Jobs", enriched)

st.divider()

# ── Top Skills ────────────────────────────────────────────────────────────────
st.header("🔧 Top In-Demand Skills")
st.caption("Skills most frequently mentioned in job postings (AI-extracted)")

top_skills = (
    skills_df.groupby("skill")["job_count"]
    .sum()
    .sort_values(ascending=False)
    .head(25)
    .reset_index()
)
st.bar_chart(top_skills.set_index("skill")["job_count"])

st.divider()

# ── Skills by Seniority ───────────────────────────────────────────────────────
st.header("👤 Skills by Seniority Level")

if not seniority_df.empty:
    levels = sorted(seniority_df["seniority"].unique().tolist())
    selected = st.selectbox("Select seniority level", levels)
    filtered = (
        seniority_df[seniority_df["seniority"] == selected]
        .sort_values("job_count", ascending=False)
        .head(20)
    )
    if not filtered.empty:
        st.bar_chart(filtered.set_index("skill")["job_count"])
    else:
        st.info("No data for this seniority level.")

st.divider()

# ── Top Companies ─────────────────────────────────────────────────────────────
st.header("🏢 Most Active Hiring Companies")

top_cos = companies_df.sort_values("job_count", ascending=False).head(20)
st.bar_chart(top_cos.set_index("company_name")["job_count"])

st.divider()

# ── Latest Jobs ───────────────────────────────────────────────────────────────
st.header("📋 Latest Job Postings")

display_cols = ["title", "company_name", "location_name", "role_family", "seniority", "posted_date"]
available = [c for c in display_cols if c in jobs_df.columns]
st.dataframe(jobs_df[available].head(50), use_container_width=True, hide_index=True)

st.divider()
st.caption("Built with Python · DuckDB · Ollama · Streamlit | Data from Remotive & RemoteOK APIs")
