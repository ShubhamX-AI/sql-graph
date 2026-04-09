# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

NL2SQL Setup Pipeline — a two-phase tool that reads a MySQL schema, enriches it with OpenAI, discovers table relationships, stores everything in Neo4j, and then answers natural language questions with accurate SQL.

## Setup

```bash
# Install dependencies (Python 3.12 required)
uv sync

# Configure credentials
cp .env.example .env
# Fill in MYSQL_*, OPENAI_API_KEY, NEO4J_* values
```

## Running

```bash
# Phase 1: One-time pipeline (extract → enrich → store in Neo4j)
uv run python run_pipeline.py

# Phase 2: Query
uv run python query.py "Show me top 10 customers by revenue this year"
```

The pipeline is resumable — `pipeline_progress.json` tracks completed tables. Delete this file to reprocess all tables from scratch.

## Architecture

The system has two distinct phases that share the Neo4j graph as the handoff point.

### Pipeline phase (`run_pipeline.py` orchestrates):

1. **`pipeline/extractor.py`** — reads MySQL `information_schema` to produce `RawTable` / `RawColumn` dataclasses with schema, sample rows, FK constraints, and PK metadata.

2. **`db/connection.py`** — owns the shared MySQL connection helper used by both the pipeline and query-time SQL execution.

3. **`pipeline/enricher.py`** — sends each `RawTable` to OpenAI (`gpt-5-mini` by default) with a structured prompt asking for human-readable names, descriptions, and example questions. Returns `EnrichedTable` / `EnrichedColumn` dataclasses. The model must return raw JSON (no markdown fences).

4. **`pipeline/relationships.py`** — three-layer discovery returning `Relationship` dataclasses:
   - Layer 1: FK constraints from `information_schema` (confidence 1.0)
   - Layer 2: Columns sharing the same `human_name` across tables (confidence 0.7)
   - Layer 3: Value overlap sampling to confirm Layer 2 (boosts confidence to ~0.9, discards if below `VALUE_OVERLAP_THRESHOLD`)

5. **`graph/store.py` (`SchemaGraph`)** — writes to Neo4j:
   - `(:Table)` nodes with OpenAI vector embeddings (default `text-embedding-3-small`, 1536-dim)
   - `(:Column)` nodes linked via `[:HAS_COLUMN]`
   - `[:REFERENCES]` edges between Column nodes (with `join_condition`, `confidence`, `source`)
   - `[:RELATED_TO]` shortcut edges between Table nodes (for fast traversal at query time)

### Query phase (`query.py`)

1. Embeds the question → vector similarity search against the configured Neo4j vector index
2. Fetches columns for those tables + JOIN paths (direct and 1-hop bridge paths)
3. Calls OpenAI with the focused schema prompt to generate a MySQL `SELECT` query
4. Executes the SQL (read-only MySQL connection)
5. Calls OpenAI again to summarize the result in plain English

### Key dataclass flow

```text
RawTable → EnrichedTable → Neo4j nodes → prompt context → SQL → summary
```

## Configuration (`config.py`)

All tunable parameters live here, read from `.env`:
- `SAMPLE_ROWS` — rows sampled per table during enrichment (default 5)
- `VALUE_OVERLAP_THRESHOLD` — minimum overlap ratio to confirm a name-match relationship (default 0.6)
- `MAX_CARDINALITY_FOR_OVERLAP` — skip overlap check for high-cardinality columns like UUIDs (default 500)
- `LLM_CALL_DELAY` — seconds between OpenAI API calls to avoid rate limits (default 1.0)
- `EMBEDDING_MODEL` / `EMBEDDING_DIMS` — OpenAI embedding model and vector size
- `VECTOR_INDEX_NAME` / `VECTOR_PROPERTY_NAME` — Neo4j vector index/property names used by `SchemaGraph`

## Coding Style

- The main coding agents are software engineers, so write code accordingly.
- Senior software engineers prefer simpler, more readable code over complex solutions.
- Move implementation toward simplicity, not complexity.
- Fix the exact thing you were asked to fix, not other unrelated things.

## Important notes

- The canonical Neo4j module is [store.py](/home/shubhan_halder/CODE/Onespace/sql-graph/graph/store.py). Import `SchemaGraph` with `from graph.store import SchemaGraph`.
- The default OpenAI model is controlled via `OPENAI_MODEL` in `.env`.
- The default embedding model is controlled via `OPENAI_EMBEDDING_MODEL` and `OPENAI_EMBEDDING_DIMS`.
- MySQL access is strictly read-only throughout — no writes occur.
- Neo4j requires the vector index feature (available in Neo4j 5.x Enterprise or AuraDB).
