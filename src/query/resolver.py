from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

from rapidfuzz import fuzz

import src.core.config as config


TEXT_DATA_TYPES = {
    "char",
    "varchar",
    "text",
    "tinytext",
    "mediumtext",
    "longtext",
    "enum",
    "set",
}


@dataclass(frozen=True)
class LookupCandidate:
    table_name: str
    column_name: str
    raw_value: str
    reason: str = ""


@dataclass(frozen=True)
class ResolvedValue:
    table_name: str
    column_name: str
    original_value: str
    resolved_value: str
    score: float
    strategy: str


_CANDIDATE_CACHE: dict[tuple[str, str], tuple[float, list[str]]] = {}


class ValueResolver:
    def __init__(self, conn: Any):
        self._conn = conn

    def resolve(
        self,
        lookups: list[LookupCandidate],
        column_map: dict[tuple[str, str], dict[str, Any]],
    ) -> list[ResolvedValue]:
        resolved: list[ResolvedValue] = []
        for lookup in lookups:
            result = self.resolve_one(lookup, column_map)
            if result is not None:
                resolved.append(result)
        return resolved

    def resolve_one(
        self,
        lookup: LookupCandidate,
        column_map: dict[tuple[str, str], dict[str, Any]],
    ) -> ResolvedValue | None:
        column = column_map.get((lookup.table_name, lookup.column_name))
        if column is None or not is_text_like_column(column):
            return None

        if len(lookup.raw_value.strip()) < config.FUZZY_MATCH_MIN_VALUE_LENGTH:
            return None

        candidates = self._get_distinct_values(lookup.table_name, lookup.column_name)
        if not candidates:
            return None

        return _pick_candidate(lookup, candidates)

    def _get_distinct_values(self, table_name: str, column_name: str) -> list[str]:
        cache_key = (table_name, column_name)
        cached = _CANDIDATE_CACHE.get(cache_key)
        now = time.monotonic()
        if cached is not None and cached[0] > now:
            return cached[1]

        cur = self._conn.cursor()
        cur.execute(
            (
                f"SELECT DISTINCT `{column_name}` "
                f"FROM `{table_name}` "
                f"WHERE `{column_name}` IS NOT NULL "
                f"LIMIT %s"
            ),
            (config.FUZZY_MATCH_MAX_CANDIDATES + 1,),
        )
        rows = cur.fetchall()
        values = [str(row[0]).strip() for row in rows if row and row[0] is not None]

        if len(values) > config.FUZZY_MATCH_MAX_CANDIDATES:
            return []

        _CANDIDATE_CACHE[cache_key] = (
            now + config.FUZZY_MATCH_CACHE_TTL_SECONDS,
            values,
        )
        return values


def build_column_map(columns: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (column["table_name"], column["raw_name"]): column
        for column in columns
    }


def build_text_column_summary(columns: list[dict[str, Any]]) -> str:
    grouped: dict[str, list[str]] = {}
    for column in columns:
        if is_text_like_column(column):
            grouped.setdefault(column["table_name"], []).append(column["raw_name"])

    if not grouped:
        return "No text-like columns were found in the retrieved schema."

    lines: list[str] = []
    for table_name in sorted(grouped):
        lines.append(f"- {table_name}: {', '.join(sorted(grouped[table_name]))}")
    return "\n".join(lines)


def is_text_like_column(column: dict[str, Any]) -> bool:
    return str(column.get("data_type", "")).lower() in TEXT_DATA_TYPES


def normalize_text(value: str) -> str:
    normalized = value.casefold().strip()
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _pick_candidate(lookup: LookupCandidate, candidates: list[str]) -> ResolvedValue | None:
    original = lookup.raw_value.strip()
    normalized_original = normalize_text(original)
    if not normalized_original:
        return None

    normalized_candidates = {
        candidate: normalize_text(candidate)
        for candidate in candidates
        if candidate.strip()
    }

    for candidate, normalized in normalized_candidates.items():
        if normalized == normalized_original:
            return ResolvedValue(
                table_name=lookup.table_name,
                column_name=lookup.column_name,
                original_value=original,
                resolved_value=candidate,
                score=100.0,
                strategy="normalized_exact",
            )

    partial_matches = [
        candidate
        for candidate, normalized in normalized_candidates.items()
        if normalized.startswith(normalized_original)
        or normalized_original.startswith(normalized)
        or normalized_original in normalized
        or normalized in normalized_original
    ]
    if len(partial_matches) == 1:
        return ResolvedValue(
            table_name=lookup.table_name,
            column_name=lookup.column_name,
            original_value=original,
            resolved_value=partial_matches[0],
            score=95.0,
            strategy="normalized_partial",
        )

    scored_matches = [
        (candidate, _normalized_similarity(normalized_original, normalized))
        for candidate, normalized in normalized_candidates.items()
        if normalized
    ]
    if not scored_matches:
        return None

    scored_matches.sort(key=lambda item: item[1], reverse=True)
    best_candidate, best_score = scored_matches[0]
    second_score = scored_matches[1][1] if len(scored_matches) > 1 else 0
    if best_score < config.FUZZY_MATCH_MIN_SCORE:
        return None
    if best_score - second_score < config.FUZZY_MATCH_MIN_LEAD:
        return None

    return ResolvedValue(
        table_name=lookup.table_name,
        column_name=lookup.column_name,
        original_value=original,
        resolved_value=best_candidate,
        score=float(best_score),
        strategy="rapidfuzz",
    )


def _normalized_similarity(
    query: str,
    choice: str,
    *,
    score_cutoff: float = 0,
) -> float:
    score = max(
        fuzz.ratio(query, choice),
        fuzz.partial_ratio(query, choice),
        fuzz.token_sort_ratio(query, choice),
    )
    if score < score_cutoff:
        return 0
    return float(score)
