# graph/store.py
# All Neo4j operations: setup, writing enriched schema, and query-time retrieval.

from __future__ import annotations
from neo4j import GraphDatabase

from src.pipeline.enricher import EnrichedTable
from src.pipeline.relationships import Relationship
import src.core.config as config
import src.core.openai_client as openai_client


class SchemaGraph:

    def __init__(self):
        self._driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        )
        self._openai = openai_client.create_client()

    def close(self):
        self._driver.close()

    # ── One-time setup ──────────────────────────────────────────────────────────

    def create_indexes(self):
        """Run once before loading data."""
        with self._driver.session() as s:
            # Vector index for semantic table search
            s.run(f"""
                CREATE VECTOR INDEX {config.VECTOR_INDEX_NAME} IF NOT EXISTS
                FOR (t:Table) ON (t.{config.VECTOR_PROPERTY_NAME})
                OPTIONS {{
                    indexConfig: {{
                        `vector.dimensions`: {config.EMBEDDING_DIMS},
                        `vector.similarity_function`: 'cosine'
                    }}
                }}
            """)
            # Fast lookup indexes
            s.run("CREATE INDEX table_name_idx IF NOT EXISTS FOR (t:Table) ON (t.raw_name)")
            s.run("CREATE INDEX col_idx IF NOT EXISTS FOR (c:Column) ON (c.raw_name, c.table_name)")
        print("Neo4j indexes ready.")

    # ── Writing data ────────────────────────────────────────────────────────────

    def store_table(self, table: EnrichedTable):
        embedding = self._make_table_embedding(table)
        with self._driver.session() as s:
            s.execute_write(self._write_table, table, embedding)
            for col in table.columns:
                s.execute_write(self._write_column, table.raw_name, col)

    def store_relationship(self, rel: Relationship):
        with self._driver.session() as s:
            s.execute_write(self._write_relationship, rel)

    # ── Query-time retrieval ─────────────────────────────────────────────────────

    def retrieve_context(self, question: str, top_k: int = 6) -> str:
        """
        Given a natural language question, returns a ready-to-use prompt
        section containing relevant table schemas + join conditions.
        """
        q_embedding = self._make_embedding(question)

        with self._driver.session() as s:
            tables      = s.execute_read(self._vector_search, q_embedding, top_k)
            table_names = [t["raw_name"] for t in tables]
            columns     = s.execute_read(self._get_columns_for_tables, table_names)
            joins       = s.execute_read(self._get_join_paths, table_names)

        return _build_prompt_block(tables, columns, joins)

    # ── Neo4j write transactions ─────────────────────────────────────────────────

    @staticmethod
    def _write_table(tx, table: EnrichedTable, embedding: list[float]):
        tx.run("""
            MERGE (t:Table {raw_name: $raw_name})
            SET t.human_name     = $human_name,
                t.description    = $description,
                t.common_queries = $common_queries,
                t.""" + config.VECTOR_PROPERTY_NAME + """ = $embedding
        """,
            raw_name      = table.raw_name,
            human_name    = table.human_name,
            description   = table.description,
            common_queries= table.common_queries,
            embedding     = embedding,
        )

    @staticmethod
    def _write_column(tx, table_name: str, col):
        tx.run("""
            MATCH (t:Table {raw_name: $table_name})
            MERGE (c:Column {raw_name: $raw_name, table_name: $table_name})
            SET c.human_name   = $human_name,
                c.data_type    = $data_type,
                c.description  = $description,
                c.is_pk        = $is_pk,
                c.is_fk        = $is_fk
            MERGE (t)-[:HAS_COLUMN]->(c)
        """,
            table_name = table_name,
            raw_name   = col.raw_name,
            human_name = col.human_name,
            data_type  = col.data_type,
            description= col.description,
            is_pk      = col.is_primary_key,
            is_fk      = col.is_foreign_key,
        )

    @staticmethod
    def _write_relationship(tx, rel: Relationship):
        # REFERENCES edge between Column nodes (detailed)
        tx.run("""
            MATCH (c1:Column {raw_name: $from_col, table_name: $from_table})
            MATCH (c2:Column {raw_name: $to_col,   table_name: $to_table})
            MERGE (c1)-[r:REFERENCES]->(c2)
            SET r.join_condition = $join_condition,
                r.confidence     = $confidence,
                r.source         = $source
        """,
            from_col       = rel.from_col,
            from_table     = rel.from_table,
            to_col         = rel.to_col,
            to_table       = rel.to_table,
            join_condition = rel.join_condition,
            confidence     = rel.confidence,
            source         = rel.source,
        )
        # RELATED_TO shortcut directly between Table nodes (fast traversal at query time)
        tx.run("""
            MATCH (t1:Table {raw_name: $from_table})
            MATCH (t2:Table {raw_name: $to_table})
            MERGE (t1)-[r:RELATED_TO]->(t2)
            SET r.join_condition = $join_condition,
                r.confidence     = $confidence
        """,
            from_table     = rel.from_table,
            to_table       = rel.to_table,
            join_condition = rel.join_condition,
            confidence     = rel.confidence,
        )

    # ── Neo4j read transactions ──────────────────────────────────────────────────

    @staticmethod
    def _vector_search(tx, embedding: list[float], top_k: int) -> list[dict]:
        result = tx.run("""
            CALL db.index.vector.queryNodes($index_name, $top_k, $embedding)
            YIELD node AS t, score
            WHERE score > 0.5
            RETURN t.raw_name   AS raw_name,
                   t.human_name AS human_name,
                   t.description AS description,
                   score
            ORDER BY score DESC
        """, index_name=config.VECTOR_INDEX_NAME, embedding=embedding, top_k=top_k)
        return result.data()

    @staticmethod
    def _get_columns_for_tables(tx, table_names: list[str]) -> list[dict]:
        result = tx.run("""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE t.raw_name IN $tables
            RETURN t.raw_name    AS table_name,
                   c.raw_name    AS raw_name,
                   c.human_name  AS human_name,
                   c.data_type   AS data_type,
                   c.description AS description,
                   c.is_pk       AS is_pk,
                   c.is_fk       AS is_fk
            ORDER BY table_name, raw_name
        """, tables=table_names)
        return result.data()

    @staticmethod
    def _get_join_paths(tx, table_names: list[str]) -> list[dict]:
        # Direct joins between the retrieved tables
        result = tx.run("""
            MATCH (t1:Table)-[r:RELATED_TO]->(t2:Table)
            WHERE t1.raw_name IN $tables AND t2.raw_name IN $tables
            RETURN t1.raw_name AS from_table,
                   t2.raw_name AS to_table,
                   r.join_condition AS join_condition,
                   r.confidence AS confidence
            ORDER BY r.confidence DESC
        """, tables=table_names)
        direct = result.data()

        # 1-hop bridge paths: retrieved table → bridge table → retrieved table
        result2 = tx.run("""
            MATCH (t1:Table)-[r1:RELATED_TO]->(bridge:Table)-[r2:RELATED_TO]->(t2:Table)
            WHERE t1.raw_name IN $tables
              AND t2.raw_name IN $tables
              AND NOT bridge.raw_name IN $tables
            RETURN t1.raw_name     AS from_table,
                   bridge.raw_name AS via_table,
                   t2.raw_name     AS to_table,
                   r1.join_condition AS join1,
                   r2.join_condition AS join2,
                   (r1.confidence + r2.confidence) / 2 AS confidence
            ORDER BY confidence DESC
            LIMIT 10
        """, tables=table_names)
        bridge = result2.data()

        return direct + bridge

    # ── Embedding helper ─────────────────────────────────────────────────────────

    def _make_table_embedding(self, table: EnrichedTable) -> list[float]:
        text = (
            f"Table: {table.human_name}\n"
            f"Purpose: {table.description}\n"
            f"Columns: {', '.join(c.human_name for c in table.columns)}\n"
            f"Common questions: {' | '.join(table.common_queries)}"
        )
        return self._make_embedding(text)

    def _make_embedding(self, text: str) -> list[float]:
        return openai_client.create_embedding(
            self._openai,
            text=text,
            model=config.EMBEDDING_MODEL,
            dimensions=config.EMBEDDING_DIMS,
        )


# ── Prompt assembly ──────────────────────────────────────────────────────────────

def _build_prompt_block(
    tables:  list[dict],
    columns: list[dict],
    joins:   list[dict],
) -> str:
    # Group columns by table
    col_map: dict[str, list[dict]] = {}
    for c in columns:
        col_map.setdefault(c["table_name"], []).append(c)

    lines = ["=== RELEVANT SCHEMA ===\n"]
    for t in tables:
        tname = t["raw_name"]
        lines.append(f"Table: {tname}  ({t['human_name']})")
        lines.append(f"Purpose: {t['description']}")
        lines.append("Columns:")
        for c in col_map.get(tname, []):
            flags = ""
            if c["is_pk"]: flags += " [PK]"
            if c["is_fk"]: flags += " [FK]"
            lines.append(f"  {c['raw_name']}  ({c['human_name']}, {c['data_type']}){flags}")
            if c.get("description"):
                lines.append(f"    → {c['description']}")
        lines.append("")

    if joins:
        lines.append("=== JOIN CONDITIONS ===")
        for j in joins:
            if "via_table" in j:
                lines.append(
                    f"  {j['from_table']} → {j['via_table']} → {j['to_table']}"
                    f"  via: {j['join1']}  AND  {j['join2']}"
                )
            else:
                lines.append(f"  {j['join_condition']}")
        lines.append("")

    return "\n".join(lines)
