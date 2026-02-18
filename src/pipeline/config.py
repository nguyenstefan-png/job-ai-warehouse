"""
config.py — Loads settings from the .env file.
All other modules import from here instead of reading .env directly.
"""
import os
from dotenv import load_dotenv

# Load variables from .env into the environment
load_dotenv()

DUCKDB_PATH   = os.getenv("DUCKDB_PATH", "./data/jobs.duckdb")
USE_OLLAMA    = os.getenv("USE_OLLAMA", "true").lower() == "true"
OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "llama3.1")
OLLAMA_URL    = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
