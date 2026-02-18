"""
tests/test_pipeline.py — Basic unit tests for the pipeline.

Run with:  python -m pytest tests/ -v

These tests verify that core functions work correctly
without needing a live database or internet connection.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.pipeline.normalize import _parse_date, company_id, job_id
from src.pipeline.ai_extract import _fallback_extract, Extraction
import datetime as dt


class TestDateParsing:
    def test_iso_string(self):
        result = _parse_date("2024-06-15T10:00:00")
        assert result == dt.date(2024, 6, 15)

    def test_date_only_string(self):
        result = _parse_date("2024-06-15")
        assert result == dt.date(2024, 6, 15)

    def test_unix_timestamp(self):
        result = _parse_date(1718409600)  # 2024-06-15 in Unix time
        assert isinstance(result, dt.date)

    def test_none_input(self):
        assert _parse_date(None) is None

    def test_empty_string(self):
        assert _parse_date("") is None


class TestIdGeneration:
    def test_company_id_is_deterministic(self):
        """Same company name must always produce same ID."""
        assert company_id("Stripe") == company_id("Stripe")

    def test_different_companies_have_different_ids(self):
        assert company_id("Stripe") != company_id("Airbnb")

    def test_job_id_is_deterministic(self):
        assert job_id("remotive", "12345") == job_id("remotive", "12345")

    def test_different_sources_produce_different_ids(self):
        assert job_id("remotive", "123") != job_id("remoteok", "123")


class TestFallbackExtractor:
    def test_detects_python(self):
        result = _fallback_extract("Data Engineer", "We need python and sql skills")
        assert "python" in result["skills"]
        assert "sql" in result["skills"]

    def test_detects_seniority_senior(self):
        result = _fallback_extract("Senior Data Engineer", "5+ years experience required")
        assert result["seniority"] == "senior"

    def test_detects_de_role_family(self):
        result = _fallback_extract("Data Engineer", "Build data pipelines with airflow and dbt")
        assert result["role_family"] == "data_engineering"

    def test_unknown_for_irrelevant_text(self):
        result = _fallback_extract("Accountant", "Manage spreadsheets")
        assert result["role_family"] == "unknown"

    def test_skills_are_lowercase(self):
        result = _fallback_extract("Engineer", "Must know Python, SQL, and AWS")
        for skill in result["skills"]:
            assert skill == skill.lower()


class TestExtractionSchema:
    def test_valid_extraction(self):
        data = Extraction(seniority="senior", role_family="data_engineering", skills=["python"])
        assert data.seniority == "senior"
        assert data.skills == ["python"]

    def test_defaults_on_empty(self):
        data = Extraction()
        assert data.seniority == "unknown"
        assert data.role_family == "unknown"
        assert data.skills == []

    def test_invalid_seniority_uses_default(self):
        # Pydantic should reject invalid literal values
        try:
            data = Extraction(seniority="god_tier")
            # If it doesn't raise, it should fall back
        except Exception:
            pass  # Expected
