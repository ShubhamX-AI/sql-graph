# query.py
# Query-time usage: takes a natural language question, retrieves context
# from the Neo4j graph, and calls OpenAI to generate and run the SQL.
#
# Usage:
#   python query.py "Show me total revenue per customer this month"

import sys
from openai import OpenAI

import src.core.config as config
from src.db.connection import connect
from src.graph.store import SchemaGraph
import src.core.openai_client as openai_client
from src.query.resolver import (
    LookupCandidate,
    ResolvedValue,
    ValueResolver,
    build_column_map,
    build_text_column_summary,
)


SYSTEM_PROMPT = """You are an expert SQL analyst for a MySQL database.
You will be given:
  1. A schema section with the relevant tables and columns
  2. The known JOIN conditions between those tables
  3. A natural language question from a user

Your job is to write a single, correct MySQL SELECT query that answers the question.

Rules:
- Use ONLY the tables and columns provided in the schema section
- Use the exact raw column names (not the human-readable names in parentheses)
- Use the JOIN conditions exactly as provided — do not guess joins
- Always use table aliases to avoid ambiguity
- If a Resolved values section is provided, treat those canonical values as the best spelling for the user intent
- Prefer exact equality for high-confidence resolved full-field entity values
- For unresolved text filters, use LIKE with % wildcards to handle partial matches
- Return ONLY the SQL query, nothing else — no explanation, no markdown fences
"""

VALUE_LOOKUP_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "lookups": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "table_name": {"type": "string"},
                    "column_name": {"type": "string"},
                    "raw_value": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["table_name", "column_name", "raw_value", "reason"],
            },
        },
    },
    "required": ["lookups"],
}

VALUE_LOOKUP_INSTRUCTIONS = """You extract likely text filter values from a user question.

Rules:
- Only return lookups for text-like columns explicitly listed in the available text columns section.
- Only return values the user actually mentioned or clearly implied.
- Do not invent tables, columns, or values.
- Prefer entity-like filters such as names, emails, cities, companies, products, and statuses.
- Return an empty lookups array when no clear text filter is present.
"""


def ask(question: str) -> str:
    # Step 1: Retrieve relevant schema from Neo4j
    graph = SchemaGraph()
    context_bundle = graph.retrieve_context_bundle(question, top_k=6)
    graph.close()
    schema_context = context_bundle["prompt"]
    columns = context_bundle["columns"]

    # Step 2: Resolve likely text values before SQL generation
    client = openai_client.create_client()
    conn = connect()
    try:
        resolved_values: list[ResolvedValue] = []
        if config.ENABLE_FUZZY_VALUE_RESOLUTION:
            lookups = _extract_value_lookups(question, schema_context, columns, client)
            resolver = ValueResolver(conn)
            resolved_values = resolver.resolve(lookups, build_column_map(columns))

        # Step 3: Call OpenAI to generate SQL
        sql = _clean_sql(
            openai_client.generate_text(
                client,
                instructions=SYSTEM_PROMPT,
                prompt=_build_sql_prompt(question, schema_context, resolved_values),
                max_output_tokens=1000,
            )
        )

        # Step 4: Run the SQL on MySQL (read-only connection)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    # Step 5: Format result back into natural language
    result_text = _format_result(rows, sql)
    summary = _summarize(question, sql, result_text, client)
    return summary


def _format_result(rows: list[dict], sql: str) -> str:
    if not rows:
        return "No rows returned."
    headers = list(rows[0].keys())
    lines = [" | ".join(headers)]
    lines.append("-" * len(lines[0]))
    for row in rows[:50]:   # cap at 50 rows for the summary
        lines.append(" | ".join(str(row[h]) for h in headers))
    if len(rows) > 50:
        lines.append(f"... and {len(rows) - 50} more rows")
    return "\n".join(lines)


def _summarize(
    question:    str,
    sql:         str,
    result_text: str,
    client:      OpenAI,
) -> str:
    summary = openai_client.generate_text(
        client,
        prompt=(
            f"The user asked: {question}\n\n"
            f"I ran this SQL:\n{sql}\n\n"
            f"Results:\n{result_text}\n\n"
            "Write a concise, friendly 2-3 sentence summary of the answer."
        ),
        max_output_tokens=500,
    )
    return f"{summary}\n\n(SQL used: {sql})"


def _clean_sql(sql: str) -> str:
    sql = sql.strip()
    if sql.startswith("```"):
        sql = sql.split("```", 2)[1]
        if sql.startswith("sql"):
            sql = sql[3:]
    return sql.strip()


def _extract_value_lookups(
    question: str,
    schema_context: str,
    columns: list[dict],
    client: OpenAI,
) -> list[LookupCandidate]:
    text_column_summary = build_text_column_summary(columns)
    if text_column_summary.startswith("No text-like columns"):
        return []

    prompt = (
        f"{schema_context}\n\n"
        "=== AVAILABLE TEXT COLUMNS ===\n"
        f"{text_column_summary}\n\n"
        f"Question: {question}"
    )
    data = openai_client.generate_json(
        client,
        prompt=prompt,
        instructions=VALUE_LOOKUP_INSTRUCTIONS,
        schema_name="value_lookups",
        schema=VALUE_LOOKUP_SCHEMA,
        max_output_tokens=500,
    )

    valid_columns = set(build_column_map(columns))
    lookups: list[LookupCandidate] = []
    seen: set[tuple[str, str, str]] = set()
    for item in data["lookups"]:
        key = (item["table_name"], item["column_name"])
        raw_value = item["raw_value"].strip()
        if key not in valid_columns or not raw_value:
            continue
        dedupe_key = (item["table_name"], item["column_name"], raw_value.casefold())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        lookups.append(LookupCandidate(**item))
    return lookups


def _build_sql_prompt(
    question: str,
    schema_context: str,
    resolved_values: list[ResolvedValue],
) -> str:
    lines = [schema_context]
    if resolved_values:
        lines.append("=== RESOLVED VALUES ===")
        for resolved in resolved_values:
            lines.append(
                (
                    f"- {resolved.table_name}.{resolved.column_name}: "
                    f"user said '{resolved.original_value}', "
                    f"canonical value '{resolved.resolved_value}', "
                    f"score {resolved.score:.1f}, strategy {resolved.strategy}"
                )
            )
    lines.append(f"Question: {question}")
    return "\n\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python query.py "your question here"')
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"\nQuestion: {question}\n")
    print(ask(question))
