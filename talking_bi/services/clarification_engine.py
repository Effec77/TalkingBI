from typing import List, Dict, Any

def generate_clarifications(query: str, profile: Dict[str, Dict[str, Any]], missing: List[str]) -> List[str]:
    """Phase 9C.3 - Generate deterministic suggestions for INCOMPLETE intents."""
    suggestions = []

    if "kpi" in missing:
        kpis = [c for c, m in profile.items() if m.get("role_scores", {}).get("is_kpi") == 1.0]
        dims = [c for c, m in profile.items() if m.get("role_scores", {}).get("is_dimension") == 1.0]
        dim_ex = dims[0] if dims else "department"
        for k in kpis[:3]:
            suggestions.append(f"show {k} by {dim_ex}")

    if "dimension" in missing:
        # Get dimensions sorted by cardinality_bucket or just grab the first few
        dims = [c for c, m in profile.items() if m.get("role_scores", {}).get("is_dimension") == 1.0]
        for d in dims[:3]:
            # Try to grab a strong KPI to pair it with for the suggestion
            kpis = [c for c, m in profile.items() if m.get("role_scores", {}).get("is_kpi") == 1.0]
            kpi_str = kpis[0] if kpis else "revenue"
            suggestions.append(f"show {kpi_str} by {d}")

    return suggestions
