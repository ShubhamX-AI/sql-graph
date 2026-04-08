# pipeline/relationships.py
# Discovers table-to-table relationships using 3 layers:
#   Layer 1: FK constraints already declared in the DB (confidence 1.0)
#   Layer 2: Columns with same human_name across tables (confidence 0.7)
#   Layer 3: Value overlap sampling to confirm layer-2 candidates (boosts to 0.9)

from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from .enricher import EnrichedTable
import src.core.config as config


@dataclass
class Relationship:
    from_table:     str
    from_col:       str
    to_table:       str
    to_col:         str
    join_condition: str     # ready to paste into SQL
    confidence:     float
    source:         str     # "fk" | "name_match" | "name_match+value_overlap"


def discover_all(
    enriched: list[EnrichedTable],
    conn,                           # MySQL connection for value overlap
) -> list[Relationship]:

    layer1 = _from_fk_constraints(enriched)
    layer2_candidates = _from_name_matching(enriched)
    layer3 = _confirm_via_value_overlap(layer2_candidates, conn)

    # Merge: keep layer1 as-is, add confirmed layer2/3 matches
    # Deduplicate by (from_table, from_col, to_table, to_col)
    seen: dict[tuple, Relationship] = {}
    for rel in layer1 + layer3:
        key = (rel.from_table, rel.from_col, rel.to_table, rel.to_col)
        existing = seen.get(key)
        if existing is None or rel.confidence > existing.confidence:
            seen[key] = rel

    return list(seen.values())


# ── Layer 1: FK constraints ────────────────────────────────────────────────────

def _from_fk_constraints(enriched: list[EnrichedTable]) -> list[Relationship]:
    rels = []
    for table in enriched:
        for fk in table.raw_table.fk_constraints:
            rels.append(Relationship(
                from_table     = fk.from_table,
                from_col       = fk.from_col,
                to_table       = fk.to_table,
                to_col         = fk.to_col,
                join_condition = f"{fk.from_table}.{fk.from_col} = {fk.to_table}.{fk.to_col}",
                confidence     = 1.0,
                source         = "fk",
            ))
    return rels


# ── Layer 2: Column name matching ─────────────────────────────────────────────

def _from_name_matching(enriched: list[EnrichedTable]) -> list[Relationship]:
    # Build: human_name → [(table_name, raw_col_name, is_pk), ...]
    name_index: dict[str, list[tuple]] = defaultdict(list)
    for table in enriched:
        for col in table.columns:
            key = col.human_name.lower().strip()
            name_index[key].append((table.raw_name, col.raw_name, col.is_primary_key))

    candidates = []
    for human_name, occurrences in name_index.items():
        if len(occurrences) < 2:
            continue

        # Find the "owner" table — the one where this column is a PK
        pk_owners  = [(t, c) for t, c, is_pk in occurrences if is_pk]
        non_owners = [(t, c) for t, c, is_pk in occurrences if not is_pk]

        if not pk_owners:
            # No declared PK — still a candidate but lower confidence
            pk_owners  = [(occurrences[0][0], occurrences[0][1])]
            non_owners = [(t, c) for t, c, _ in occurrences[1:]]

        for to_table, to_col in pk_owners:
            for from_table, from_col in non_owners:
                if from_table == to_table:
                    continue
                candidates.append(Relationship(
                    from_table     = from_table,
                    from_col       = from_col,
                    to_table       = to_table,
                    to_col         = to_col,
                    join_condition = f"{from_table}.{from_col} = {to_table}.{to_col}",
                    confidence     = 0.7,
                    source         = "name_match",
                ))
    return candidates


# ── Layer 3: Value overlap sampling ───────────────────────────────────────────

def _confirm_via_value_overlap(
    candidates: list[Relationship],
    conn,
) -> list[Relationship]:
    confirmed = []
    for rel in candidates:
        overlap = _compute_overlap(
            conn,
            rel.from_table, rel.from_col,
            rel.to_table,   rel.to_col,
        )
        if overlap is None:
            # Could not sample (high-cardinality or error) — keep with original confidence
            confirmed.append(rel)
        elif overlap >= config.VALUE_OVERLAP_THRESHOLD:
            rel.confidence = min(1.0, 0.7 + overlap * 0.3)
            rel.source = "name_match+value_overlap"
            confirmed.append(rel)
        # else: overlap too low — discard this candidate
    return confirmed


def _compute_overlap(conn, table_a, col_a, table_b, col_b) -> float | None:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(DISTINCT `{col_a}`) FROM `{table_a}`")
        cardinality = cur.fetchone()[0]
        if cardinality > config.MAX_CARDINALITY_FOR_OVERLAP:
            return None  # Too many distinct values to be a useful FK

        cur.execute(
            f"SELECT DISTINCT `{col_a}` FROM `{table_a}` LIMIT %s",
            (config.VALUE_OVERLAP_SAMPLE,)
        )
        vals_a = {str(r[0]) for r in cur.fetchall() if r[0] is not None}

        cur.execute(
            f"SELECT DISTINCT `{col_b}` FROM `{table_b}` LIMIT %s",
            (config.VALUE_OVERLAP_SAMPLE,)
        )
        vals_b = {str(r[0]) for r in cur.fetchall() if r[0] is not None}

        if not vals_a or not vals_b:
            return None

        return len(vals_a & vals_b) / min(len(vals_a), len(vals_b))

    except Exception:
        return None
