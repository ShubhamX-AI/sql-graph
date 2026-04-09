from __future__ import annotations

import json
import random
import string
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from synthetic_data.seed_models import ColumnMeta, ForeignKeyMeta


def build_row(
    table_name: str,
    row_number: int,
    columns: list[ColumnMeta],
    fk_by_column: dict[str, ForeignKeyMeta],
    reference_values: dict[tuple[str, str], list[Any]],
    rng: random.Random,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for column in columns:
        row[column.column_name] = generate_value(
            table_name=table_name,
            column=column,
            row_number=row_number,
            fk=fk_by_column.get(column.column_name),
            reference_values=reference_values,
            rng=rng,
        )
    return row


def generate_value(
    table_name: str,
    column: ColumnMeta,
    row_number: int,
    fk: ForeignKeyMeta | None,
    reference_values: dict[tuple[str, str], list[Any]],
    rng: random.Random,
) -> Any:
    if fk is not None:
        candidates = reference_values.get(
            (fk.referenced_table_name, fk.referenced_column_name),
            [],
        )
        if candidates:
            return rng.choice(candidates)
        if column.is_nullable:
            return None

    if column.is_nullable and rng.random() < 0.1:
        return None

    column_name = column.column_name.lower()
    data_type = column.data_type.lower()

    enum_values = parse_enum_values(column.column_type)
    if enum_values:
        return rng.choice(enum_values)

    if data_type in {"tinyint"} and column.column_type.lower() == "tinyint(1)":
        return rng.choice([0, 1])
    if "bool" in column_name or column_name.startswith("is_") or column_name.startswith("has_"):
        return rng.choice([0, 1])
    if "email" in column_name:
        return f"user{row_number + 1}_{rng.randint(1000, 9999)}@example.com"
    if "first_name" in column_name:
        return rng.choice(FIRST_NAMES)
    if "last_name" in column_name:
        return rng.choice(LAST_NAMES)
    if "full_name" in column_name or column_name == "name":
        return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    if "city" in column_name:
        return rng.choice(CITIES)
    if "country" in column_name:
        return rng.choice(COUNTRIES)
    if "state" in column_name:
        return rng.choice(STATES)
    if "phone" in column_name or "mobile" in column_name:
        return fake_phone(rng)
    if "address" in column_name:
        return f"{rng.randint(10, 9999)} {rng.choice(STREET_NAMES)} St"
    if "postal" in column_name or "zip" in column_name:
        return f"{rng.randint(10000, 99999)}"
    if "company" in column_name:
        return f"{rng.choice(LAST_NAMES)} {rng.choice(COMPANY_SUFFIXES)}"
    if "status" in column_name:
        return rng.choice(STATUSES)
    if "category" in column_name or "type" in column_name:
        return rng.choice(CATEGORIES)
    if "title" in column_name:
        return rng.choice(TITLES)
    if "description" in column_name or data_type in {"text", "tinytext", "mediumtext", "longtext"}:
        return make_sentence(rng, 12)
    if "code" in column_name or "sku" in column_name:
        return f"{table_name[:3].upper()}-{rng.randint(10000, 99999)}"
    if "url" in column_name:
        return f"https://example.com/{table_name}/{row_number + 1}"

    if data_type in {"char", "varchar"}:
        max_length = parse_varchar_length(column.column_type)
        value = f"{table_name}_{column.column_name}_{row_number + 1}"
        return value[:max_length] if max_length else value
    if data_type in {"json"}:
        return json.dumps(
            {
                "source": "synthetic_data",
                "table": table_name,
                "row": row_number + 1,
            }
        )
    if data_type in {"int", "integer", "smallint", "mediumint", "bigint"}:
        return rng.randint(1, 5000)
    if data_type in {"decimal", "numeric"}:
        return Decimal(f"{rng.randint(10, 5000)}.{rng.randint(0, 99):02d}")
    if data_type in {"float", "double", "real"}:
        return round(rng.uniform(10.0, 5000.0), 2)
    if data_type == "date":
        return date.today() - timedelta(days=rng.randint(0, 730))
    if data_type in {"datetime", "timestamp"}:
        return datetime.utcnow() - timedelta(
            days=rng.randint(0, 365),
            minutes=rng.randint(0, 1440),
        )
    if data_type == "time":
        return time(
            hour=rng.randint(0, 23),
            minute=rng.randint(0, 59),
            second=rng.randint(0, 59),
        )
    if data_type == "year":
        return rng.randint(2020, date.today().year)
    if data_type in {"blob", "binary", "varbinary"}:
        return bytes(f"seed-{row_number + 1}", "utf-8")

    return random_token(rng, 12)


def parse_enum_values(column_type: str) -> list[str]:
    lowered = column_type.lower()
    if not lowered.startswith("enum("):
        return []
    inner = column_type[5:-1]
    values = []
    for chunk in inner.split(","):
        chunk = chunk.strip()
        if len(chunk) >= 2 and chunk[0] == "'" and chunk[-1] == "'":
            values.append(chunk[1:-1])
    return values


def parse_varchar_length(column_type: str) -> int | None:
    if "(" not in column_type or ")" not in column_type:
        return None
    inside = column_type.split("(", 1)[1].split(")", 1)[0]
    return int(inside) if inside.isdigit() else None


def random_token(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(rng.choice(alphabet) for _ in range(length))


def fake_phone(rng: random.Random) -> str:
    return f"+1-{rng.randint(200, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"


def make_sentence(rng: random.Random, word_count: int) -> str:
    words = [rng.choice(WORDS) for _ in range(word_count)]
    return " ".join(words).capitalize() + "."


FIRST_NAMES = [
    "Ava",
    "Liam",
    "Mia",
    "Noah",
    "Emma",
    "Lucas",
    "Olivia",
    "Ethan",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Brown",
    "Taylor",
    "Martinez",
    "Anderson",
    "Jackson",
    "Lee",
]

CITIES = [
    "New York",
    "San Francisco",
    "Chicago",
    "Austin",
    "Seattle",
    "Boston",
    "Denver",
    "Miami",
]

COUNTRIES = [
    "United States",
    "Canada",
    "United Kingdom",
    "Germany",
    "India",
]

STATES = [
    "California",
    "Texas",
    "New York",
    "Washington",
    "Illinois",
]

STREET_NAMES = [
    "Oak",
    "Maple",
    "Pine",
    "Cedar",
    "Lakeview",
    "Hillcrest",
]

COMPANY_SUFFIXES = [
    "Labs",
    "Systems",
    "Works",
    "Partners",
    "Group",
]

STATUSES = [
    "active",
    "inactive",
    "pending",
    "completed",
    "cancelled",
]

CATEGORIES = [
    "standard",
    "premium",
    "internal",
    "external",
    "enterprise",
]

TITLES = [
    "Sales Lead",
    "Project Manager",
    "Data Analyst",
    "Operations Specialist",
    "Support Engineer",
]

WORDS = [
    "adaptive",
    "platform",
    "customer",
    "workflow",
    "quality",
    "reporting",
    "regional",
    "insight",
    "delivery",
    "service",
    "planning",
    "metric",
    "revenue",
    "inventory",
    "priority",
]
