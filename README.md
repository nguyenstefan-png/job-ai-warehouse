# Job Market AI Warehouse
 
An end-to-end data engineering pipeline that ingests live job postings,
enriches them with AI-extracted skills and seniority, and serves
analytics through a Streamlit dashboard.
 
## Architecture
API (Remotive, RemoteOK) --> Raw Layer (DuckDB) --> Silver Layer --> Gold Marts --> Dashboard
 
## Key DE Features
- Idempotent ingestion (re-runs never duplicate data)
- Raw -> Silver -> Gold data layering
- AI enrichment via local LLM (Ollama llama3.1)
- Data quality checks with structured logging
- Star schema: dim_company, dim_location, dim_role, fact_job_posting
 
## Tech Stack
Python, DuckDB, Ollama, Streamlit, Pydantic, Requests
 
## How to Run
pip install -r requirements.txt
ollama pull llama3.1
python -m src.pipeline.run
streamlit run dashboard/app.py

  [Remotive API]    [RemoteOK API]
        |                  |
        +--------+---------+
                 |
           [RAW Layer]
         raw_job_postings
                 |
           [SILVER Layer]
    dim_company, dim_location,
    dim_role, fact_job_posting
         bridge_job_skill
                 |
         [AI Enrichment]
          Ollama llama3.1
     (skills, seniority, role family)
                 |
           [GOLD Layer]
    mart_top_skills_daily
    mart_skill_by_seniority
    mart_top_companies
                 |
         [Streamlit Dashboard]

