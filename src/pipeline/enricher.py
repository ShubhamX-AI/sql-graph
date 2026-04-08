# pipeline/enricher.py
# Calls OpenAI once per table to produce a human-readable description.
# Output feeds both the embedding step and the SQL generation prompt.

from __future__ import annotations
import time
from dataclasses import dataclass, field
from openai import OpenAI

from .extractor import RawTable
import src.core.config as config
import src.core.openai_client as openai_client


@dataclass
class EnrichedColumn:
    raw_name:    str
    human_name:  str        # e.g. "customer code"
    data_type:   str
    description: str        # e.g. "FK to cust_mst, identifies the customer"
    is_primary_key: bool
    is_foreign_key: bool


@dataclass
class EnrichedTable:
    raw_name:       str
    human_name:     str         # e.g. "Project Master"
    description:    str         # 2-3 sentence purpose
    columns:        list[EnrichedColumn]
    common_queries: list[str]   # example questions this table answers
    raw_table:      RawTable = field(repr=False)  # keep original for overlap checks


ENRICHMENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "human_name": {"type": "string"},
        "description": {"type": "string"},
        "common_queries": {
            "type": "array",
            "items": {"type": "string"},
        },
        "columns": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "raw_name":    {"type": "string"},
                    "human_name":  {"type": "string"},
                    "description": {"type": "string"},
                },
                "required": ["raw_name", "human_name", "description"],
            },
        },
    },
    "required": ["human_name", "description", "common_queries", "columns"],
}


def enrich_table(raw: RawTable, client: OpenAI) -> EnrichedTable:
    prompt = _build_prompt(raw)
    data = openai_client.generate_json(
        client,
        prompt=prompt,
        schema_name="table_enrichment",
        schema=ENRICHMENT_SCHEMA,
        max_output_tokens=1500,
    )
    time.sleep(config.LLM_CALL_DELAY)
    return _parse_response(data, raw)


# ── prompt building ────────────────────────────────────────────────────────────

def _build_prompt(raw: RawTable) -> str:
    col_lines = "\n".join(
        f"  {c.name}  ({c.data_type})"
        + ("  [PK]" if c.is_primary_key else "")
        + ("  [FK]" if c.is_foreign_key else "")
        + (f"  -- {c.column_comment}" if c.column_comment else "")
        for c in raw.columns
    )

    sample_text = ""
    if raw.sample_rows:
        headers = list(raw.sample_rows[0].keys())
        sample_text = "\nSAMPLE DATA:\n"
        sample_text += " | ".join(headers) + "\n"
        sample_text += "-" * 60 + "\n"
        for row in raw.sample_rows:
            sample_text += " | ".join(str(row.get(h, ""))[:30] for h in headers) + "\n"

    fk_text = ""
    if raw.fk_constraints:
        fk_text = "\nDECLARED FOREIGN KEYS:\n"
        for fk in raw.fk_constraints:
            fk_text += f"  {fk.from_col} → {fk.to_table}.{fk.to_col}\n"

    table_comment = f"\nDB TABLE COMMENT: {raw.table_comment}" if raw.table_comment else ""

    return f"""You are a senior data analyst. Analyze this database table and return a JSON description.
The column names are often short abbreviations — infer their real meaning from the data types, sample values, and context.

TABLE: {raw.name}{table_comment}

COLUMNS:
{col_lines}
{fk_text}{sample_text}

Return ONLY valid JSON in exactly this format (no markdown, no extra text):
{{
  "human_name": "Short readable table name (2-4 words)",
  "description": "2-3 sentences explaining what this table stores and its business purpose",
  "common_queries": [
    "Example natural language question this table answers",
    "Another example question",
    "Another example question"
  ],
  "columns": [
    {{
      "raw_name": "exact column name from above",
      "human_name": "readable name (2-4 words, lowercase)",
      "description": "what this column stores, including FK target if applicable"
    }}
  ]
}}"""


# ── response parsing ───────────────────────────────────────────────────────────

def _parse_response(data: dict, raw: RawTable) -> EnrichedTable:
    col_map = {c["raw_name"]: c for c in data["columns"]}
    enriched_cols = []
    for raw_col in raw.columns:
        llm_col = col_map.get(raw_col.name, {})
        enriched_cols.append(EnrichedColumn(
            raw_name       = raw_col.name,
            human_name     = llm_col.get("human_name", raw_col.name),
            data_type      = raw_col.data_type,
            description    = llm_col.get("description", ""),
            is_primary_key = raw_col.is_primary_key,
            is_foreign_key = raw_col.is_foreign_key,
        ))

    return EnrichedTable(
        raw_name       = raw.name,
        human_name     = data["human_name"],
        description    = data["description"],
        columns        = enriched_cols,
        common_queries = data.get("common_queries", []),
        raw_table      = raw,
    )
