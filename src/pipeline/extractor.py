# pipeline/extractor.py
# Pulls everything we need from MySQL.
# Produces a list of RawTable objects — input to the enricher.

from __future__ import annotations
from dataclasses import dataclass, field
import src.core.config as config


@dataclass
class RawColumn:
    name: str
    data_type: str
    is_nullable: bool
    column_comment: str        # DB-level column comment (often blank)
    is_primary_key: bool = False
    is_foreign_key: bool = False


@dataclass
class FKConstraint:
    from_table: str
    from_col:   str
    to_table:   str
    to_col:     str


@dataclass
class RawTable:
    name: str
    table_comment: str          # DB-level table comment (often blank)
    columns: list[RawColumn]
    sample_rows: list[dict]     # Up to SAMPLE_ROWS actual data rows
    fk_constraints: list[FKConstraint] = field(default_factory=list)

def extract_all_tables(conn) -> list[RawTable]:
    tables = _get_table_names(conn)
    fk_map = _get_all_fk_constraints(conn)
    pk_map = _get_all_primary_keys(conn)

    result = []
    for table_name in tables:
        columns   = _get_columns(conn, table_name, pk_map, fk_map)
        samples   = _get_sample_rows(conn, table_name)
        comment   = _get_table_comment(conn, table_name)
        fks       = fk_map.get(table_name, [])
        result.append(RawTable(
            name           = table_name,
            table_comment  = comment,
            columns        = columns,
            sample_rows    = samples,
            fk_constraints = fks,
        ))
    return result


# ── private helpers ────────────────────────────────────────────────────────────

def _get_table_names(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """, (config.MYSQL_DATABASE,))
    return [row[0] for row in cur.fetchall()]


def _get_table_comment(conn, table_name: str) -> str:
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_COMMENT
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (config.MYSQL_DATABASE, table_name))
    row = cur.fetchone()
    return (row[0] or "").strip() if row else ""


def _get_all_primary_keys(conn) -> dict[str, set[str]]:
    """Returns {table_name: {pk_col1, pk_col2, ...}}"""
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
          AND CONSTRAINT_NAME = 'PRIMARY'
    """, (config.MYSQL_DATABASE,))
    pk_map: dict[str, set[str]] = {}
    for table, col in cur.fetchall():
        pk_map.setdefault(table, set()).add(col)
    return pk_map


def _get_all_fk_constraints(conn) -> dict[str, list[FKConstraint]]:
    """Returns {from_table: [FKConstraint, ...]}"""
    cur = conn.cursor()
    cur.execute("""
        SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME,
               kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.TABLE_CONSTRAINTS tc
          ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
        WHERE kcu.TABLE_SCHEMA             = %s
          AND tc.CONSTRAINT_TYPE           = 'FOREIGN KEY'
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
    """, (config.MYSQL_DATABASE,))
    fk_map: dict[str, list[FKConstraint]] = {}
    for from_t, from_c, to_t, to_c in cur.fetchall():
        fk_map.setdefault(from_t, []).append(
            FKConstraint(from_t, from_c, to_t, to_c)
        )
    return fk_map


def _get_columns(
    conn,
    table_name: str,
    pk_map: dict[str, set[str]],
    fk_map: dict[str, list[FKConstraint]],
) -> list[RawColumn]:
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (config.MYSQL_DATABASE, table_name))

    pk_cols = pk_map.get(table_name, set())
    fk_cols = {fk.from_col for fk in fk_map.get(table_name, [])}

    return [
        RawColumn(
            name           = row[0],
            data_type      = row[1],
            is_nullable    = row[2] == "YES",
            column_comment = (row[3] or "").strip(),
            is_primary_key = row[0] in pk_cols,
            is_foreign_key = row[0] in fk_cols,
        )
        for row in cur.fetchall()
    ]


def _get_sample_rows(conn, table_name: str) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SELECT * FROM `{table_name}` LIMIT {config.SAMPLE_ROWS}")
        return cur.fetchall()
    except Exception:
        # Some tables may not be readable (views, permission issues)
        return []
