from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnMeta:
    table_name: str
    column_name: str
    data_type: str
    column_type: str
    is_nullable: bool
    column_default: Any
    extra: str
    column_key: str
    ordinal_position: int


@dataclass(frozen=True)
class ForeignKeyMeta:
    table_name: str
    column_name: str
    referenced_table_name: str
    referenced_column_name: str
