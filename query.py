# query.py
# Query-time usage: takes a natural language question, retrieves context
# from the Neo4j graph, and calls OpenAI to generate and run the SQL.
#
# Usage:
#   python query.py "Show me total revenue per customer this month"

import sys
from openai import OpenAI

from pipeline import extractor
from graph.store import SchemaGraph
import core.openai_client as openai_client


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
- For text filters, use LIKE with % wildcards to handle partial matches
- Return ONLY the SQL query, nothing else — no explanation, no markdown fences
"""


def ask(question: str) -> str:
    # Step 1: Retrieve relevant schema from Neo4j
    graph = SchemaGraph()
    schema_context = graph.retrieve_context(question, top_k=6)
    graph.close()

    # Step 2: Call OpenAI to generate SQL
    client = openai_client.create_client()
    sql = _clean_sql(
        openai_client.generate_text(
            client,
            instructions=SYSTEM_PROMPT,
            prompt=f"{schema_context}\n\nQuestion: {question}",
            max_output_tokens=1000,
        )
    )

    # Step 3: Run the SQL on MySQL (read-only connection)
    conn = extractor.connect()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    # Step 4: Format result back into natural language
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python query.py "your question here"')
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"\nQuestion: {question}\n")
    print(ask(question))
