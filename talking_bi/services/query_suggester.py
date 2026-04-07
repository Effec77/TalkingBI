from __future__ import annotations

from typing import Any, Dict, List, Tuple


MAX_SUGGESTIONS = 8


def _is_identifier(col: str, meta: Dict[str, Any]) -> bool:
    name = (col or "").lower()
    if name.endswith("_id") or name == "id":
        return True
    return str(meta.get("semantic_type", "")).lower() == "identifier"


def _extract_components(profile: Dict[str, Dict[str, Any]]) -> Tuple[List[str], List[str], List[str]]:
    kpis: List[str] = []
    dimensions: List[str] = []
    time_cols: List[str] = []

    for col, meta in profile.items():
        role = meta.get("role_scores", {}) or {}
        if _is_identifier(col, meta):
            continue

        if float(role.get("is_kpi", 0.0)) == 1.0:
            kpis.append(col)
        if float(role.get("is_dimension", 0.0)) == 1.0:
            dimensions.append(col)
        # Support either key naming.
        is_time = float(role.get("is_time", role.get("is_date", 0.0)))
        if is_time == 1.0:
            time_cols.append(col)

    return kpis, dimensions, time_cols


def _suggestion_score(
    profile: Dict[str, Dict[str, Any]],
    kpi: str,
    dimension: str | None = None,
    with_time: bool = False,
) -> float:
    k_meta = profile.get(kpi, {})
    k_role = k_meta.get("role_scores", {}) or {}
    score = float(k_role.get("is_kpi", 0.0))

    if dimension:
        d_meta = profile.get(dimension, {})
        bucket = str(d_meta.get("cardinality_bucket", "")).lower()
        if bucket == "low":
            score += 0.35
        elif bucket == "med":
            score += 0.20

    if with_time:
        score += 0.25

    return score


def generate_suggestions(profile: Dict[str, Dict[str, Any]], prefix: str = "") -> Dict[str, List[str]]:
    """
    Deterministic query suggestion generation from DIL profile.
    """
    kpis, dimensions, time_cols = _extract_components(profile)
    if not kpis:
        return {"suggestions": []}

    scored: List[Tuple[float, str]] = []

    # Template 1: show {kpi}
    for kpi in kpis:
        scored.append((_suggestion_score(profile, kpi), f"show {kpi}"))

    # Template 2: show {kpi} by {dimension}
    for kpi in kpis:
        for dim in dimensions[:2]:
            scored.append((_suggestion_score(profile, kpi, dimension=dim), f"show {kpi} by {dim}"))

    # Template 3: show {kpi} over time
    if time_cols:
        for kpi in kpis:
            scored.append((_suggestion_score(profile, kpi, with_time=True), f"show {kpi} over time"))

    # Template 4: compare {kpi1} with {kpi2}
    if len(kpis) >= 2:
        for i in range(len(kpis)):
            for j in range(i + 1, len(kpis)):
                k1 = kpis[i]
                k2 = kpis[j]
                pair_score = (_suggestion_score(profile, k1) + _suggestion_score(profile, k2)) / 2
                scored.append((pair_score, f"compare {k1} with {k2}"))

    # Deduplicate while keeping best score for each suggestion.
    best: Dict[str, float] = {}
    for score, s in scored:
        if s not in best or score > best[s]:
            best[s] = score

    ranked = sorted(best.items(), key=lambda x: (-x[1], x[0]))
    suggestions = [suggestion for suggestion, _score in ranked]

    if prefix:
        p = prefix.strip().lower()
        suggestions = [s for s in suggestions if s.lower().startswith(p)]

    return {"suggestions": suggestions[:MAX_SUGGESTIONS]}
