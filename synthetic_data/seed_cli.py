from __future__ import annotations

import argparse
import random

import mysql.connector

from synthetic_data.seed_connection import connect_for_seed
from synthetic_data.seed_demo_schema import (
    DEMO_TABLE_NAMES,
    create_demo_schema,
    parse_demo_selected_tables,
)
from synthetic_data.seed_runner import seed_tables
from synthetic_data.seed_schema import (
    load_reference_values,
    load_schema,
    order_tables,
    parse_selected_tables,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed a MySQL schema with simple synthetic data."
    )
    parser.add_argument(
        "--rows-per-table",
        type=int,
        default=25,
        help="How many rows to add to each targeted table.",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated list of tables to seed. Defaults to all tables.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used for repeatable synthetic data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without inserting rows.",
    )
    parser.add_argument(
        "--create-demo-schema",
        action="store_true",
        help="Create a fixed demo schema if the target database has no tables.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    printed_connection = False
    try:
        conn = connect_for_seed()
    except mysql.connector.Error as exc:
        raise SystemExit(
            "Unable to connect to MySQL for seeding. "
            "Set a valid write-enabled connection in MYSQL_SEED_* "
            "or update MYSQL_* in .env. "
            f"Driver error: {exc}"
        ) from exc

    try:
        tables, columns_by_table, foreign_keys = load_schema(conn)

        if args.create_demo_schema and tables:
            raise SystemExit(
                "Refusing to create the demo schema because the target database "
                "already has tables. Use an empty database or run without "
                "--create-demo-schema."
            )

        if not tables:
            if not args.create_demo_schema:
                print(f"Connected to MySQL database: {conn.database}")
                printed_connection = True
                print("")
                print(
                    "No tables found in this database. The seeder only populates "
                    "existing tables."
                )
                print(
                    "Create/import your schema first, or run again with "
                    "--create-demo-schema to create sample tables."
                )
                return

            selected_demo_tables = parse_demo_selected_tables(args.tables)
            print(f"Connected to MySQL database: {conn.database}")
            printed_connection = True
            if args.dry_run:
                demo_table_order = [
                    table for table in DEMO_TABLE_NAMES if table in selected_demo_tables
                ]
                print("Demo schema would be created: " + ", ".join(DEMO_TABLE_NAMES))
                print(f"Target tables: {', '.join(demo_table_order)}")
                print(f"Rows per table: {args.rows_per_table}")
                print("Dry run enabled: no tables or rows will be created.")
                print("")
                print("Seed summary")
                print("------------")
                for table_name in demo_table_order:
                    print(f"{table_name}: {args.rows_per_table} row(s)")
                return

            create_demo_schema(conn)
            print("Created demo schema: " + ", ".join(DEMO_TABLE_NAMES))
            tables, columns_by_table, foreign_keys = load_schema(conn)
            selected_tables = selected_demo_tables
        else:
            selected_tables = parse_selected_tables(args.tables, tables)

        table_order = order_tables(tables, foreign_keys, selected_tables)
        reference_values = load_reference_values(conn, foreign_keys, selected_tables)

        if not printed_connection:
            print(f"Connected to MySQL database: {conn.database}")
        print(f"Target tables: {', '.join(table_order)}")
        print(f"Rows per table: {args.rows_per_table}")
        if args.dry_run:
            print("Dry run enabled: no rows will be inserted.")

        results = seed_tables(
            conn=conn,
            table_order=table_order,
            columns_by_table=columns_by_table,
            foreign_keys=foreign_keys,
            reference_values=reference_values,
            rows_per_table=args.rows_per_table,
            rng=rng,
            dry_run=args.dry_run,
        )

        print("")
        print("Seed summary")
        print("------------")
        for table_name, inserted in results:
            print(f"{table_name}: {inserted} row(s)")
    finally:
        conn.close()
