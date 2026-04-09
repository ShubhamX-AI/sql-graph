from __future__ import annotations

import random
from typing import Any

import mysql.connector

from synthetic_data.seed_generator import build_row
from synthetic_data.seed_models import ColumnMeta, ForeignKeyMeta


def seed_tables(
    conn: mysql.connector.MySQLConnection,
    table_order: list[str],
    columns_by_table: dict[str, list[ColumnMeta]],
    foreign_keys: dict[str, list[ForeignKeyMeta]],
    reference_values: dict[tuple[str, str], list[Any]],
    rows_per_table: int,
    rng: random.Random,
    dry_run: bool,
) -> list[tuple[str, int]]:
    results: list[tuple[str, int]] = []

    for table_name in table_order:
        columns = columns_by_table.get(table_name, [])
        if not columns:
            results.append((table_name, 0))
            continue

        fk_by_column = {fk.column_name: fk for fk in foreign_keys.get(table_name, [])}
        insertable_columns = [column for column in columns if is_insertable(column)]
        if not insertable_columns:
            results.append((table_name, 0))
            continue

        rows = [
            build_row(
                table_name=table_name,
                row_number=index,
                columns=insertable_columns,
                fk_by_column=fk_by_column,
                reference_values=reference_values,
                rng=rng,
            )
            for index in range(rows_per_table)
        ]

        if dry_run:
            results.append((table_name, len(rows)))
            continue

        inserted = insert_rows(conn, table_name, insertable_columns, rows)
        refresh_reference_values(
            conn=conn,
            table_name=table_name,
            foreign_keys=foreign_keys,
            reference_values=reference_values,
        )
        results.append((table_name, inserted))

    return results


def is_insertable(column: ColumnMeta) -> bool:
    extra = column.extra.lower()
    if "auto_increment" in extra:
        return False
    if "generated" in extra:
        return False
    if column.data_type == "timestamp" and column.column_default is not None:
        return False
    return True


def insert_rows(
    conn: mysql.connector.MySQLConnection,
    table_name: str,
    columns: list[ColumnMeta],
    rows: list[dict[str, Any]],
) -> int:
    cursor = conn.cursor()
    column_names = [column.column_name for column in columns]
    placeholders = ", ".join(["%s"] * len(column_names))
    quoted_columns = ", ".join(f"`{name}`" for name in column_names)
    sql = f"INSERT INTO `{table_name}` ({quoted_columns}) VALUES ({placeholders})"
    values = [tuple(row[name] for name in column_names) for row in rows]
    cursor.executemany(sql, values)
    conn.commit()
    inserted = cursor.rowcount
    cursor.close()
    return inserted


def refresh_reference_values(
    conn: mysql.connector.MySQLConnection,
    table_name: str,
    foreign_keys: dict[str, list[ForeignKeyMeta]],
    reference_values: dict[tuple[str, str], list[Any]],
) -> None:
    cursor = conn.cursor()
    targets = {
        (fk.referenced_table_name, fk.referenced_column_name)
        for fks in foreign_keys.values()
        for fk in fks
        if fk.referenced_table_name == table_name
    }
    for parent_table, parent_column in targets:
        cursor.execute(
            f"SELECT DISTINCT `{parent_column}` FROM `{parent_table}` "
            f"WHERE `{parent_column}` IS NOT NULL LIMIT 1000"
        )
        reference_values[(parent_table, parent_column)] = [
            row[0] for row in cursor.fetchall()
        ]
    cursor.close()
