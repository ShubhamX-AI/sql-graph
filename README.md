# NL2SQL Setup Pipeline

> One-time setup that reads your MySQL schema, enriches it with OpenAI,
> discovers table relationships, and stores everything in Neo4j so a
> chatbot can answer natural language questions with accurate SQL.

## Getting Started

```bash
# 1. Install dependencies
uv sync

# 2. Set up credentials
cp .env.example .env
# Fill in your MySQL, OpenAI, and Neo4j credentials

# 3. Run the one-time setup pipeline (~1-2 min per 10 tables)
uv run python run_pipeline.py

# 4. Query your database in plain English
uv run python query.py "Show me top 10 customers by revenue this year"
uv run python query.py "How many active projects are there per city?"
```

## Synthetic Data

To populate a local MySQL dev database with synthetic rows, use the generic
seeder in [synthetic_data/seed_mysql.py](/home/shubham_halder/CODE/ONESPACE/sql-graph/synthetic_data/seed_mysql.py).

```bash
uv run python synthetic_data/seed_mysql.py --rows-per-table 25
```

The seeder reads your existing schema, orders tables using foreign-key
dependencies where possible, and generates fake values based on column names
and data types. Use `MYSQL_SEED_*` environment variables for a write-enabled
dev user if your normal `MYSQL_*` credentials are read-only. If the target
database is empty, run `uv run python synthetic_data/seed_mysql.py
--create-demo-schema` to create a small demo schema
(`customers`, `products`, `orders`, `order_items`) before seeding.

## Project Architecture

```text
sql-graph/
├── README.md               # This file
├── pyproject.toml          # Project metadata + dependencies
├── uv.lock                 # Locked dependency versions
├── .env.example            # Credential template — copy to .env
│
├── config.py               # All settings (reads from .env)
├── openai_client.py        # Shared OpenAI text/json/embedding helpers
├── run_pipeline.py         # One-time setup orchestrator
├── query.py                # Ask natural language questions
│
├── db/
│   └── connection.py       # Shared MySQL connection helper
│
├── pipeline/
│   ├── extractor.py        # Pull raw schema + sample rows from MySQL
│   ├── enricher.py         # Calls OpenAI to make schema human-readable
│   └── relationships.py    # 3-layer relationship discovery
│
└── graph/
    └── store.py            # Neo4j storage + query-time retrieval
```

## How It Works

### One-time setup (`run_pipeline.py`)

```text
MySQL DB
  │
  ├─ db/connection.py
  │    → shared MySQL connection setup
  │
  ├─ pipeline/extractor.py
  │    → raw schema: tables, columns, types, sample rows, FK constraints
  │
  ├─ pipeline/enricher.py
  │    → calls OpenAI per table
  │    → human-readable descriptions + column mappings
  │
  ├─ pipeline/relationships.py
  │    → 3-layer discovery:
  │      Layer 1: FK constraints from information_schema (confidence 1.0)
  │      Layer 2: Columns with same human name across tables (confidence 0.7)
  │      Layer 3: Value overlap sampling to confirm layer 2 (confidence 0.9)
  │
  └─ graph/store.py
       → stores in Neo4j:
         (:Table) nodes with vector embeddings
         (:Column) nodes with types and descriptions
         [:HAS_COLUMN] Table → Column
         [:REFERENCES] Column → Column
         [:RELATED_TO] Table → Table
```

### At query time (`query.py`)

```text
User question
  │
  ├─ Embed question → vector similarity search → top-6 relevant Table nodes
  ├─ Graph traversal → JOIN paths between those tables
  ├─ Build focused schema prompt (only relevant tables, exact join conditions)
  ├─ OpenAI generates SQL
  ├─ Run SQL on MySQL (read-only)
  └─ OpenAI summarizes result in plain English
```

## Key Design Decisions

- **Resumable pipeline**: `pipeline_progress.json` tracks which tables are done. If the run crashes, re-run and it skips already-processed tables.
- **OpenAI embeddings**: Uses `text-embedding-3-small` by default for semantic schema search. The configured vector size defaults to 1536 dimensions.
- **Read-only MySQL**: The pipeline and query engine only read from MySQL. No writes, no risk.
- **Confidence-scored relationships**: Each edge has a confidence score. FK constraints = 1.0, name match + value overlap = ~0.9, name match only = 0.7.

If you are migrating from the previous local embedding setup, clear `pipeline_progress.json` and rebuild the Neo4j table embeddings so every table gets indexed with the OpenAI vector format.

## Dependencies

Dependencies are managed through [pyproject.toml](/home/shubhan_halder/CODE/Onespace/sql-graph/pyproject.toml) and [uv.lock](/home/shubhan_halder/CODE/Onespace/sql-graph/uv.lock).

- `mysql-connector-python`: MySQL connection
- `openai`: OpenAI API for schema enrichment, embeddings, and SQL generation
- `neo4j`: Graph database driver
- `rapidfuzz`: Fuzzy string matching for value resolver
- `python-dotenv`: Load credentials from `.env`
- `tqdm`: Progress bars
