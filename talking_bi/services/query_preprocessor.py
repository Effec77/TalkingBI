"""
Phase 9C.2: Query Preprocessor

Runs BEFORE the LLM parser to strengthen query understanding.
All rules are deterministic — same input always yields same output.

Rules applied in order:
1. Trend normalization  — "show trends" → "show trends over time"
2. Compare completion   — "compare with X" → "compare <ctx_kpi> with X"
3. Filter completion    — "filter salary" → "filter salary not null"
4. Synonym normalization — domain aliases → canonical terms

Constraints:
- NO LLM usage
- NO randomness
- NEVER strips information already present
- NEVER overrides if rule already satisfied
"""

from typing import Dict, Any, Optional

# ---------------------------------------------------------------------------
# Synonym map: user alias → canonical term used in semantic/schema layers
# Keep intentionally small — only high-confidence, universal mappings.
# ---------------------------------------------------------------------------
SYNONYMS: Dict[str, str] = {
    # Only substitute when the alias is NEVER a dataset column name and
    # the canonical form is a common BI term.
    "headcount":  "employees",
    "staff":      "employees",
    "workers":    "employees",
    "turnover":   "revenue",          # "turnover" is never a raw column; "revenue" is common
    "spend":      "cost",
    "spending":   "cost",
}


def apply_synonyms(q: str) -> str:
    """Replace known aliases with canonical terms (word-boundary safe)."""
    import re
    for alias, canonical in SYNONYMS.items():
        # Replace only whole words to avoid partial matches
        q = re.sub(rf"\b{re.escape(alias)}\b", canonical, q)
    return q


def preprocess_query(query: str, context: Optional[Dict[str, Any]] = None) -> str:
    """
    Deterministically improve query before LLM parsing.

    Args:
        query:   Raw user query string.
        context: Last resolved intent dict (may be None for first turn).

    Returns:
        Improved query string (may be identical to input if no rules fire).
    """
    context = context or {}
    q = query.strip()
    q_lower = q.lower()

    # ------------------------------------------------------------------
    # Rule 1 — Trend normalization
    # "show trends" / "revenue trends" → append "over time" so downstream
    # date-column detection is reliably triggered.
    # ------------------------------------------------------------------
    trend_keywords = ["trend", "trends"]
    has_trend     = any(kw in q_lower for kw in trend_keywords)
    has_over_time = "over time" in q_lower

    if has_trend and not has_over_time:
        q = q.rstrip() + " over time"
        q_lower = q.lower()

    # ------------------------------------------------------------------
    # Rule 2 — Compare completion
    # "compare with expenses" → "compare <ctx_kpi> with expenses"
    # Only fires when context has a resolved KPI and the pattern is present.
    # ------------------------------------------------------------------
    ctx_kpi = context.get("kpi")
    if ctx_kpi and "compare with" in q_lower and f"compare {ctx_kpi.lower()}" not in q_lower:
        q = q.replace("compare with", f"compare {ctx_kpi} with", 1)
        q_lower = q.lower()

    # ------------------------------------------------------------------
    # Rule 3 — Filter completion
    # "filter salary" (exactly 2 tokens) → "filter salary not null"
    # ------------------------------------------------------------------
    tokens = q.split()
    if tokens and tokens[0].lower() == "filter" and len(tokens) == 2:
        q = q + " not null"
        q_lower = q.lower()

    # ------------------------------------------------------------------
    # Rule 4 — Synonym normalization (applied last so rules above see
    # original user terms for pattern matching)
    # ------------------------------------------------------------------
    q = apply_synonyms(q)

    return q
