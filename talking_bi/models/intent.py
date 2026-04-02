"""
Intent Models - Phase 6B
Structured intent representation for controlled NL understanding.
"""

from typing import TypedDict, Optional


class Intent(TypedDict):
    """
    Structured intent from natural language query.

    Attributes:
        intent: The analytical intent type (from VALID_INTENTS)
        kpi: Target KPI name if specified (primary KPI)
        kpi_1: Primary KPI for COMPARE intent (resolver fills from context if null)
        kpi_2: Secondary KPI for COMPARE intent (explicit comparison target)
        dimension: Column to segment by or filter on
        filter: Filter value or condition
    """

    intent: str
    kpi: Optional[str]
    kpi_1: Optional[str]
    kpi_2: Optional[str]
    dimension: Optional[str]
    filter: Optional[str]


# Exhaustive intent taxonomy - fixed set, no open-ended interpretation
# NOTE: TOP_N deferred to Phase 6D/7 (requires dynamic KPI handling)
VALID_INTENTS = {
    "EXPLAIN_TREND",  # "why did revenue drop?"
    "SEGMENT_BY",  # "show by region"
    "FILTER",  # "show Q3 only"
    "SUMMARIZE",  # "give me a summary"
    "COMPARE",  # "compare this vs last month"
    # "TOP_N",  # DEFERRED: "top 5 products" - requires Phase 6D/7
    "UNKNOWN",  # anything else - system asks for clarification
}


# Intent descriptions for LLM prompt
# NOTE: TOP_N description kept for documentation but intent disabled
INTENT_DESCRIPTIONS = {
    "EXPLAIN_TREND": "Explain why a KPI changed (increase/decrease)",
    "SEGMENT_BY": "Break down a KPI by a dimension (region, product, etc.)",
    "FILTER": "Filter to specific time period or value",
    "SUMMARIZE": "Provide overall dashboard summary",
    "COMPARE": "Compare two time periods or segments",
    # "TOP_N": "Show top N items by value",  # DEFERRED to Phase 6D/7
    "UNKNOWN": "Query cannot be understood - needs clarification",
}
