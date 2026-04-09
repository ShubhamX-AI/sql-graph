from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

import mysql.connector

from synthetic_data.seed_models import ColumnMeta, ForeignKeyMeta


def load_schema(
    conn: mysql.connector.MySQLConnection,
) -> tuple[list[str], dict[str, list[ColumnMeta]], dict[str, list[ForeignKeyMeta]]]:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (conn.database,),
    )
    tables = [row["TABLE_NAME"] for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            EXTRA,
            COLUMN_KEY,
            ORDINAL_POSITION
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        (conn.database,),
    )
    columns: dict[str, list[ColumnMeta]] = defaultdict(list)
    for row in cursor.fetchall():
        columns[row["TABLE_NAME"]].append(
            ColumnMeta(
                table_name=row["TABLE_NAME"],
                column_name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                column_type=row["COLUMN_TYPE"],
                is_nullable=row["IS_NULLABLE"] == "YES",
                column_default=row["COLUMN_DEFAULT"],
                extra=row["EXTRA"] or "",
                column_key=row["COLUMN_KEY"] or "",
                ordinal_position=row["ORDINAL_POSITION"],
            )
        )

    cursor.execute(
        """
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY TABLE_NAME, COLUMN_NAME
        """,
        (conn.database,),
    )
    foreign_keys: dict[str, list[ForeignKeyMeta]] = defaultdict(list)
    for row in cursor.fetchall():
        foreign_keys[row["TABLE_NAME"]].append(
            ForeignKeyMeta(
                table_name=row["TABLE_NAME"],
                column_name=row["COLUMN_NAME"],
                referenced_table_name=row["REFERENCED_TABLE_NAME"],
                referenced_column_name=row["REFERENCED_COLUMN_NAME"],
            )
        )

    cursor.close()
    return tables, columns, foreign_keys


def order_tables(
    tables: list[str],
    foreign_keys: dict[str, list[ForeignKeyMeta]],
    selected_tables: set[str],
) -> list[str]:
    tables_to_seed = [table for table in tables if table in selected_tables]
    indegree = {table: 0 for table in tables_to_seed}
    edges: dict[str, set[str]] = defaultdict(set)

    for child, fks in foreign_keys.items():
        if child not in indegree:
            continue
        for fk in fks:
            parent = fk.referenced_table_name
            if parent == child or parent not in indegree:
                continue
            if child not in edges[parent]:
                edges[parent].add(child)
                indegree[child] += 1

    queue = deque(sorted(table for table, degree in indegree.items() if degree == 0))
    ordered: list[str] = []

    while queue:
        table = queue.popleft()
        ordered.append(table)
        for child in sorted(edges[table]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    remaining = sorted(table for table in tables_to_seed if table not in ordered)
    ordered.extend(remaining)
    return ordered


def load_reference_values(
    conn: mysql.connector.MySQLConnection,
    foreign_keys: dict[str, list[ForeignKeyMeta]],
    selected_tables: set[str],
) -> dict[tuple[str, str], list[Any]]:
    reference_targets = {
        (fk.referenced_table_name, fk.referenced_column_name)
        for table in selected_tables
        for fk in foreign_keys.get(table, [])
    }
    values: dict[tuple[str, str], list[Any]] = {}
    cursor = conn.cursor()

    for table_name, column_name in sorted(reference_targets):
        cursor.execute(
            f"SELECT DISTINCT `{column_name}` FROM `{table_name}` "
            f"WHERE `{column_name}` IS NOT NULL LIMIT 1000"
        )
        values[(table_name, column_name)] = [row[0] for row in cursor.fetchall()]

    cursor.close()
    return values


def parse_selected_tables(raw_tables: str, available_tables: list[str]) -> set[str]:
    if not raw_tables.strip():
        return set(available_tables)

    requested = {table.strip() for table in raw_tables.split(",") if table.strip()}
    unknown = sorted(requested - set(available_tables))
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(unknown)}")
    return requested
