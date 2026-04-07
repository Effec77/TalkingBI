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


def _low_card_dimensions(
    profile: Dict[str, Dict[str, Any]], dimensions: List[str], limit: int = 2
) -> List[str]:
    ranked: List[Tuple[int, str]] = []
    for d in dimensions:
        meta = profile.get(d, {}) or {}
        bucket = str(meta.get("cardinality_bucket", "")).lower()
        bucket_rank = 0 if bucket == "low" else 1 if bucket in {"med", "medium"} else 2
        ranked.append((bucket_rank, d))
    ranked.sort(key=lambda x: (x[0], x[1]))
    return [d for _b, d in ranked[:limit]]


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


def _sample_value_for_dimension(meta: Dict[str, Any]) -> str | None:
    samples = meta.get("sample_values", []) or []
    if not samples:
        return None
    return str(samples[0])


def _entity_label(profile: Dict[str, Dict[str, Any]]) -> str:
    names = [c.lower() for c in profile.keys()]
    for preferred in ["employee_name", "employee_id", "person", "user", "customer", "patient", "name"]:
        if any(preferred in n for n in names):
            if "patient" in preferred:
                return "patients"
            if "customer" in preferred:
                return "customers"
            if "user" in preferred:
                return "users"
            return "people"
    return "records"


def _followup_candidates(
    profile: Dict[str, Dict[str, Any]], context: Dict[str, Any]
) -> List[Tuple[float, str]]:
    kpis, dimensions, time_cols = _extract_components(profile)
    if not kpis:
        return []

    ctx_kpi = str(context.get("kpi") or "").strip().lower()
    ctx_dim = str(context.get("dimension") or "").strip().lower()

    chosen_kpi = ctx_kpi if ctx_kpi in kpis else kpis[0]
    scored: List[Tuple[float, str]] = []
    base_dims = _low_card_dimensions(profile, dimensions, limit=2)

    # keep same-kpi exploration
    scored.append((_suggestion_score(profile, chosen_kpi), f"show {chosen_kpi}"))

    # dimension drill-down
    if ctx_dim and ctx_dim in dimensions:
        scored.append(
            (_suggestion_score(profile, chosen_kpi, dimension=ctx_dim), f"show {chosen_kpi} by {ctx_dim}")
        )
        sample = _sample_value_for_dimension(profile.get(ctx_dim, {}))
        if sample:
            scored.append(
                (_suggestion_score(profile, chosen_kpi, dimension=ctx_dim) + 0.05, f"show {chosen_kpi} where {ctx_dim} = {sample}")
            )
    else:
        for dim in base_dims[:1]:
            scored.append(
                (_suggestion_score(profile, chosen_kpi, dimension=dim), f"show {chosen_kpi} by {dim}")
            )

    # time continuation
    if time_cols:
        scored.append((_suggestion_score(profile, chosen_kpi, with_time=True), f"show {chosen_kpi} over time"))

    # compare continuation (prefer dimension-qualified compare to avoid ambiguity)
    compare_dim = ctx_dim if ctx_dim and ctx_dim in dimensions else (base_dims[0] if base_dims else None)
    for alt in kpis:
        if alt == chosen_kpi:
            continue
        pair_score = (_suggestion_score(profile, chosen_kpi) + _suggestion_score(profile, alt)) / 2
        if compare_dim:
            scored.append((pair_score + 0.15, f"compare {chosen_kpi} and {alt} by {compare_dim}"))
        else:
            scored.append((pair_score + 0.08, f"compare {chosen_kpi} with {alt}"))
        break

    # dataset-query style follow-ups (top/bottom and cardinality checks)
    if base_dims:
        dim0 = base_dims[0]
        scored.append((_suggestion_score(profile, chosen_kpi, dimension=dim0) + 0.18, f"how many unique {dim0} are there"))
        scored.append((_suggestion_score(profile, chosen_kpi, dimension=dim0) + 0.12, f"list all {dim0}"))
    entity = _entity_label(profile)
    scored.append((_suggestion_score(profile, chosen_kpi) + 0.08, f"list top 5 {entity} with highest {chosen_kpi}"))
    scored.append((_suggestion_score(profile, chosen_kpi) + 0.06, f"list bottom 5 {entity} with lowest {chosen_kpi}"))

    return scored


def generate_suggestions(
    profile: Dict[str, Dict[str, Any]],
    context: Dict[str, Any] | None = None,
    prefix: str = "",
) -> Dict[str, Any]:
    """
    Deterministic query suggestion generation from DIL profile.
    """
    kpis, dimensions, time_cols = _extract_components(profile)
    if not kpis:
        return {"type": "initial", "items": [], "suggestions": []}

    scored: List[Tuple[float, str]] = []
    suggestion_type = "initial"

    if context:
        followup = _followup_candidates(profile, context)
        if followup:
            scored.extend(followup)
            suggestion_type = "followup"

    if not scored:
        base_dims = _low_card_dimensions(profile, dimensions, limit=2)

        # DAL-first discovery suggestions
        scored.append((0.95, "what metrics are available in this dataset"))
        scored.append((0.9, "list columns"))
        if base_dims:
            scored.append((1.08, f"how many unique {base_dims[0]} are there"))
            scored.append((1.02, f"list all {base_dims[0]}"))

        # Template 1: show {kpi}
        for kpi in kpis:
            scored.append((_suggestion_score(profile, kpi), f"show {kpi}"))

        # Template 2: show {kpi} by {dimension}
        for kpi in kpis:
            for dim in base_dims:
                scored.append((_suggestion_score(profile, kpi, dimension=dim), f"show {kpi} by {dim}"))
                sample = _sample_value_for_dimension(profile.get(dim, {}))
                if sample:
                    scored.append((_suggestion_score(profile, kpi, dimension=dim) + 0.08, f"show {kpi} where {dim} = {sample}"))

        # Template 3: show {kpi} over time
        if time_cols:
            for kpi in kpis:
                scored.append((_suggestion_score(profile, kpi, with_time=True), f"show {kpi} over time"))

        # Template 4: compare {kpi1} with {kpi2} (prefer dimension-qualified compare)
        if len(kpis) >= 2:
            for i in range(len(kpis)):
                for j in range(i + 1, len(kpis)):
                    k1 = kpis[i]
                    k2 = kpis[j]
                    pair_score = (_suggestion_score(profile, k1) + _suggestion_score(profile, k2)) / 2
                    if base_dims:
                        scored.append((pair_score + 0.3, f"compare {k1} and {k2} by {base_dims[0]}"))
                    else:
                        scored.append((pair_score, f"compare {k1} with {k2}"))

        # Dataset-query style templates (top N / bottom N)
        entity = _entity_label(profile)
        for kpi in kpis[:2]:
            scored.append((_suggestion_score(profile, kpi) + 0.15, f"list top 5 {entity} with highest {kpi}"))
            scored.append((_suggestion_score(profile, kpi) + 0.14, f"list bottom 5 {entity} with lowest {kpi}"))
            scored.append((_suggestion_score(profile, kpi) + 0.13, f"how many entries are in {kpi} column"))

    # Deduplicate while keeping best score for each suggestion.
    best: Dict[str, float] = {}
    for score, s in scored:
        if s not in best or score > best[s]:
            best[s] = score

    ranked = sorted(best.items(), key=lambda x: (-x[1], x[0]))
    suggestions = [suggestion for suggestion, _score in ranked]

    # Keep suggestions deterministic and "ready to run":
    # avoid duplicate compare phrasings when a by-dimension compare exists.
    has_dim_compare = any(("compare " in s and " by " in s) for s in suggestions)
    if has_dim_compare:
        suggestions = [
            s for s in suggestions
            if not ("compare " in s and " with " in s and " by " not in s)
        ]

    if prefix:
        p = prefix.strip().lower()
        suggestions = [s for s in suggestions if s.lower().startswith(p)]

    items = suggestions[:MAX_SUGGESTIONS]
    if not prefix and items:
        has_dal = any(("how many unique" in s.lower()) or ("list columns" in s.lower()) for s in items)
        if not has_dal:
            dal_fallback = next(
                (s for s in suggestions if ("how many unique" in s.lower()) or ("list columns" in s.lower())),
                None,
            )
            if dal_fallback:
                items = (items[:-1] + [dal_fallback])[:MAX_SUGGESTIONS]
    return {"type": suggestion_type, "items": items, "suggestions": items}
