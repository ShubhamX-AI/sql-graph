# config.py
# All credentials and settings. Copy .env.example to .env and fill in values.

import os
from dotenv import load_dotenv

load_dotenv()

# ── MySQL (read-only connection) ──────────────────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER     = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# ── OpenAI (schema enrichment + SQL generation) ──────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5-mini")

# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ── Pipeline settings ─────────────────────────────────────────────────────────
# How many rows to sample per table for enrichment context
SAMPLE_ROWS = 20

# Only consider a column-pair as related if value overlap exceeds this
VALUE_OVERLAP_THRESHOLD = 0.6

# How many rows to sample per column when checking value overlap
VALUE_OVERLAP_SAMPLE = 300

# Skip value overlap check for columns with more distinct values than this
# (high-cardinality columns like UUIDs will never be useful to sample)
MAX_CARDINALITY_FOR_OVERLAP = 500

# Embedding model — used for schema search in Neo4j
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIMS  = int(os.getenv("OPENAI_EMBEDDING_DIMS", 1536))
VECTOR_INDEX_NAME    = os.getenv("NEO4J_VECTOR_INDEX_NAME", "table_embeddings_openai")
VECTOR_PROPERTY_NAME = os.getenv("NEO4J_VECTOR_PROPERTY_NAME", "embedding_openai")

# ── Query-time fuzzy value resolution ─────────────────────────────────────────
ENABLE_FUZZY_VALUE_RESOLUTION = (
    os.getenv("ENABLE_FUZZY_VALUE_RESOLUTION", "true").lower() == "true"
)
FUZZY_MATCH_MIN_SCORE = int(os.getenv("FUZZY_MATCH_MIN_SCORE", 90))
FUZZY_MATCH_MIN_LEAD = int(os.getenv("FUZZY_MATCH_MIN_LEAD", 5))
FUZZY_MATCH_MAX_CANDIDATES = int(os.getenv("FUZZY_MATCH_MAX_CANDIDATES", 2000))
FUZZY_MATCH_CACHE_TTL_SECONDS = int(os.getenv("FUZZY_MATCH_CACHE_TTL_SECONDS", 300))
FUZZY_MATCH_MIN_VALUE_LENGTH = int(os.getenv("FUZZY_MATCH_MIN_VALUE_LENGTH", 3))

# Seconds to wait between LLM API calls
LLM_CALL_DELAY = 1.0
