# run_pipeline.py
# Orchestrates the full one-time setup pipeline:
#   MySQL → extract → LLM enrich → discover relationships → store in Neo4j
#
# Run: python run_pipeline.py
#
# Resumable: already-processed tables are skipped on re-run.
# Progress is saved to pipeline_progress.json

import json
import sys
from pathlib import Path

from tqdm import tqdm

import src.core.config as config
import src.core.openai_client as openai_client
from src.db.connection import connect
from src.pipeline import relationships
from src.graph.store import SchemaGraph
from src.pipeline import enricher, extractor


PROGRESS_FILE = Path("pipeline_progress.json")


def load_progress() -> set[str]:
    """Returns set of table names already successfully processed."""
    if PROGRESS_FILE.exists():
        data = json.loads(PROGRESS_FILE.read_text())
        return set(data.get("done", []))
    return set()


def save_progress(done: set[str]):
    PROGRESS_FILE.write_text(json.dumps({"done": sorted(done)}, indent=2))


def validate_config():
    required = {
        "MYSQL_USER":     config.MYSQL_USER,
        "MYSQL_PASSWORD": config.MYSQL_PASSWORD,
        "MYSQL_DATABASE": config.MYSQL_DATABASE,
        "OPENAI_API_KEY": config.OPENAI_API_KEY,
        "NEO4J_PASSWORD": config.NEO4J_PASSWORD,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in all values.")
        sys.exit(1)


def run():
    validate_config()

    print("\n=== NL2SQL Setup Pipeline ===\n")

    # ── Step 1: Connect to MySQL ─────────────────────────────────────────────
    print("Connecting to MySQL...")
    mysql_conn = connect()
    print(f"Connected to {config.MYSQL_DATABASE} @ {config.MYSQL_HOST}\n")

    # ── Step 2: Extract raw schema ───────────────────────────────────────────
    print("Extracting schema from MySQL...")
    raw_tables = extractor.extract_all_tables(mysql_conn)
    print(f"Found {len(raw_tables)} tables.\n")

    # ── Step 3: Connect to Neo4j and create indexes ──────────────────────────
    print("Connecting to Neo4j...")
    graph = SchemaGraph()
    graph.create_indexes()
    print()

    # ── Step 4: LLM enrichment (resumable) ───────────────────────────────────
    openai_api_client = openai_client.create_client()
    done_tables = load_progress()
    enriched_tables: list[enricher.EnrichedTable] = []

    remaining    = [t for t in raw_tables if t.name not in done_tables]
    already_done = len(raw_tables) - len(remaining)
    if already_done:
        print(f"Resuming — {already_done} tables already enriched, skipping them.")

    print(f"Enriching {len(remaining)} tables with OpenAI...\n")
    for raw in tqdm(remaining, desc="Enriching", unit="table"):
        try:
            enriched = enricher.enrich_table(raw, openai_api_client)
            enriched_tables.append(enriched)

            # Store in Neo4j immediately so progress is never lost
            graph.store_table(enriched)
            done_tables.add(raw.name)
            save_progress(done_tables)

        except json.JSONDecodeError as e:
            print(f"\nWARNING: OpenAI returned bad JSON for table '{raw.name}': {e}")
            print("Skipping this table — re-run to retry it.")
        except Exception as e:
            print(f"\nERROR on table '{raw.name}': {e}")
            print("Skipping — re-run to retry.")

    print(f"\nAll {len(done_tables)} tables stored in Neo4j.")

    # ── Step 5: Relationship discovery ───────────────────────────────────────
    if not enriched_tables:
        print("\nAll tables already processed. Running relationship discovery on full DB...")
        all_raw = extractor.extract_all_tables(mysql_conn)
        enriched_tables = _build_stubs_for_relationship_discovery(all_raw)

    print(f"\nDiscovering relationships across {len(enriched_tables)} tables...")
    rels = relationships.discover_all(enriched_tables, mysql_conn)
    print(f"Found {len(rels)} relationships:")
    fk_count   = sum(1 for r in rels if r.source == "fk")
    name_count = sum(1 for r in rels if "name_match" in r.source)
    print(f"  FK constraints:  {fk_count}")
    print(f"  Name + overlap:  {name_count}")

    # ── Step 6: Store relationships in Neo4j ─────────────────────────────────
    print("\nStoring relationships in Neo4j...")
    for rel in tqdm(rels, desc="Storing relationships", unit="rel"):
        graph.store_relationship(rel)

    mysql_conn.close()
    graph.close()

    print("\n=== Pipeline complete! ===")
    print(f"Tables stored:        {len(done_tables)}")
    print(f"Relationships stored: {len(rels)}")
    print("\nSchema graph is ready. Run: python query.py \"your question\"\n")


def _build_stubs_for_relationship_discovery(
    raw_tables: list[extractor.RawTable],
) -> list[enricher.EnrichedTable]:
    """
    When re-running only the relationship step, build minimal EnrichedTable
    objects from raw schema — no LLM call needed, raw column names used as-is.
    """
    stubs = []
    for raw in raw_tables:
        cols = [
            enricher.EnrichedColumn(
                raw_name       = c.name,
                human_name     = c.name,
                data_type      = c.data_type,
                description    = "",
                is_primary_key = c.is_primary_key,
                is_foreign_key = c.is_foreign_key,
            )
            for c in raw.columns
        ]
        stubs.append(enricher.EnrichedTable(
            raw_name       = raw.name,
            human_name     = raw.name,
            description    = "",
            columns        = cols,
            common_queries = [],
            raw_table      = raw,
        ))
    return stubs


if __name__ == "__main__":
    run()
