"""
Intent Parser - Phase 6B (FIXED)

Controlled natural language understanding.
LLM acts ONLY as a parser - returns structured JSON, never executes logic.

CRITICAL RULES:
1. NEVER pre-fill missing fields
2. NEVER normalize KPI names (preserve raw casing)
3. NEVER infer from context
4. COMPARE: kpi_1=null (for resolver), kpi_2=explicit target
5. TREND: Explicit keywords map to EXPLAIN_TREND
"""

import json
import re
from typing import Dict, Optional
from models.intent import Intent, VALID_INTENTS, INTENT_DESCRIPTIONS
from services.llm_manager import LLMManager


# Trend keywords for EXPLAIN_TREND detection
TREND_KEYWORDS = [
    "trend",
    "trends",
    "growth",
    "over time",
    "change over",
    "going up",
    "going down",
]

# Comparison keywords for COMPARE detection
COMPARE_KEYWORDS = ["compare", "comparison", "versus", "vs", "against"]


def _build_prompt() -> str:
    """Build strict prompt for intent parsing."""

    # Build allowed intents description
    intents_desc = "\n".join(
        [
            f"- {intent}: {INTENT_DESCRIPTIONS.get(intent, 'No description')}"
            for intent in VALID_INTENTS
        ]
    )

    prompt = f"""You are an intent parser for a BI system.

Your job: Convert natural language queries into STRICT JSON.

CRITICAL RULES:
1. Return ONLY valid JSON - no explanation, no markdown
2. Use ONLY the intent types listed below
3. Extract KPI name AS-IS (preserve casing - do not normalize)
4. Extract dimension if mentioned (dataset column name)
5. Extract filter if specified (time period, etc.)
6. NEVER pre-fill missing fields - leave as null
7. NEVER infer KPI from context - only extract what user explicitly said

INTENT DETECTION:
- COMPARE: Use when query contains: compare, versus, vs, against
- EXPLAIN_TREND: Use when query contains: trend, trends, growth, over time

ALLOWED INTENTS:
{intents_desc}

OUTPUT FORMAT:
{{
  "intent": "EXPLAIN_TREND|SEGMENT_BY|FILTER|SUMMARIZE|COMPARE|EXPLAIN_ANOMALY|UNKNOWN",
  "kpi": "KPI name exactly as stated or null",
  "kpi_1": "primary KPI for COMPARE or null", 
  "kpi_2": "comparison target for COMPARE or null",
  "dimension": "column name or null",
  "filter": "filter value or null"
}}

CRITICAL EXAMPLES:

Query: "compare with quantity"
Output: {{"intent": "COMPARE", "kpi": null, "kpi_1": null, "kpi_2": "quantity", "dimension": null, "filter": null}}

Query: "show trends"
Output: {{"intent": "EXPLAIN_TREND", "kpi": null, "kpi_1": null, "kpi_2": null, "dimension": null, "filter": null}}

Query: "show sales"
Output: {{"intent": "SEGMENT_BY", "kpi": "sales", "kpi_1": null, "kpi_2": null, "dimension": null, "filter": null}}

Query: "now by region"
Output: {{"intent": "SEGMENT_BY", "kpi": null, "kpi_1": null, "kpi_2": null, "dimension": "region", "filter": null}}

Query: "random text"
Output: {{"intent": "UNKNOWN", "kpi": null, "kpi_1": null, "kpi_2": null, "dimension": null, "filter": null}}

Now parse this query:"""

    return prompt


def _post_process_intent(query: str, intent: Dict) -> Dict:
    """
    Post-process LLM output to enforce parser rules.

    Rules:
    1. COMPARE: Ensure kpi_1 is null, kpi_2 is set (not kpi)
    2. EXPLAIN_TREND: Detect trend keywords
    3. Preserve raw KPI casing (don't capitalize)
    4. Remove any pre-filled fields not in user input
    """
    query_lower = query.lower()
    result = dict(intent)

    # Ensure all fields exist
    if "kpi" not in result:
        result["kpi"] = None
    if "kpi_1" not in result:
        result["kpi_1"] = None
    if "kpi_2" not in result:
        result["kpi_2"] = None
    if "dimension" not in result:
        result["dimension"] = None
    if "filter" not in result:
        result["filter"] = None

    intent_type = result.get("intent", "UNKNOWN")

    # FIX 1: COMPARE handling
    if intent_type == "COMPARE":
        # If LLM put KPI in wrong field, move it
        if result.get("kpi") and not result.get("kpi_2"):
            result["kpi_2"] = result["kpi"]
            result["kpi"] = None
        # Ensure kpi_1 is null (for context resolution)
        result["kpi_1"] = None

    # FIX 2: TREND detection override
    if any(kw in query_lower for kw in TREND_KEYWORDS):
        if intent_type not in ["COMPARE", "UNKNOWN"]:
            result["intent"] = "EXPLAIN_TREND"
            # Don't pre-fill KPI for trends
            if not _extract_kpi_explicit(query_lower):
                result["kpi"] = None

    # FIX 3: Preserve raw KPI casing
    # Check if KPI was extracted but should stay lowercase
    for field in ["kpi", "kpi_1", "kpi_2"]:
        if result.get(field):
            value = result[field]
            # Check if original query had lowercase version
            if value.lower() in query_lower and value != value.lower():
                # Only preserve if explicitly capitalized in query
                if value not in query:
                    result[field] = value.lower()

    # FIX 4: Remove inferred fields for partial queries
    # If query doesn't mention a KPI, ensure it's null
    if not _extract_kpi_explicit(query_lower):
        # Check intent type - only preserve KPI for specific intents
        if intent_type not in ["SEGMENT_BY", "FILTER", "EXPLAIN_TREND"]:
            pass  # Keep KPI for SUMMARIZE, COMPARE, etc.
        # For "now by X" type queries, definitely remove KPI
        if query_lower.startswith("now ") or query_lower.startswith("by "):
            result["kpi"] = None

    return result


def _extract_kpi_explicit(query_lower: str) -> bool:
    """Check if user explicitly mentioned a KPI (simple heuristic)."""
    # Common KPI indicators
    kpi_indicators = [
        "sales",
        "revenue",
        "profit",
        "quantity",
        "units",
        "customers",
        "orders",
        "price",
        "cost",
        "amount",
        "count",
        "total",
    ]
    return any(ind in query_lower for ind in kpi_indicators)


def parse_intent(query: str, llm_manager: Optional[LLMManager] = None) -> Intent:
    """
    Parse natural language query into structured intent.

    Args:
        query: Natural language query from user
        llm_manager: LLM manager instance (creates new if None)

    Returns:
        Intent dictionary with validated fields

    Note:
        This function NEVER executes logic or accesses data.
        It ONLY converts text → structured JSON.
    """
    if not query or not query.strip():
        return {
            "intent": "UNKNOWN",
            "kpi": None,
            "kpi_1": None,
            "kpi_2": None,
            "dimension": None,
            "filter": None,
        }

    # Get or create LLM manager
    if llm_manager is None:
        llm_manager = LLMManager()

    # Build prompt
    prompt = _build_prompt()

    # Initialize response for error reporting
    response = ""

    try:
        # Call LLM (single, controlled prompt)
        response = llm_manager.call_llm(prompt + f'\n\nQuery: "{query}"')

        if response is None:
            print("[INTENT_PARSER] LLM returned None")
            return {
                "intent": "UNKNOWN",
                "kpi": None,
                "kpi_1": None,
                "kpi_2": None,
                "dimension": None,
                "filter": None,
            }

        # Parse JSON response
        response_text = response.strip()

        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        response_text = response_text.strip()

        # Parse JSON
        raw_intent = json.loads(response_text)

        # Post-process to enforce rules
        intent = _post_process_intent(query, raw_intent)

        # Ensure all required fields present
        return {
            "intent": intent.get("intent", "UNKNOWN"),
            "kpi": intent.get("kpi") or None,
            "kpi_1": intent.get("kpi_1") or None,
            "kpi_2": intent.get("kpi_2") or None,
            "dimension": intent.get("dimension") or None,
            "filter": intent.get("filter") or None,
        }

    except json.JSONDecodeError as e:
        print(f"[INTENT_PARSER] JSON parse error: {e}")
        print(f"[INTENT_PARSER] Raw response: {response if response else 'None'}")
        return {
            "intent": "UNKNOWN",
            "kpi": None,
            "kpi_1": None,
            "kpi_2": None,
            "dimension": None,
            "filter": None,
        }
    except Exception as e:
        print(f"[INTENT_PARSER] Error: {e}")
        return {
            "intent": "UNKNOWN",
            "kpi": None,
            "kpi_1": None,
            "kpi_2": None,
            "dimension": None,
            "filter": None,
        }
