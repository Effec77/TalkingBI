"""
Intent Parser - Phase 6B

Controlled natural language understanding.
LLM acts ONLY as a parser - returns structured JSON, never executes logic.
"""

import json
from typing import Dict, Optional
from models.intent import Intent, VALID_INTENTS, INTENT_DESCRIPTIONS
from services.llm_manager import LLMManager


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

RULES:
1. Return ONLY valid JSON - no explanation, no markdown, no extra text
2. Use ONLY the intent types listed below
3. Extract KPI name if mentioned (exact match to planned KPIs)
4. Extract dimension if mentioned (must be a dataset column)
5. Extract filter if specified (time period, region, etc.)

ALLOWED INTENTS:
{intents_desc}

OUTPUT FORMAT:
{{
  "intent": "EXPLAIN_TREND|SEGMENT_BY|FILTER|SUMMARIZE|COMPARE|TOP_N|UNKNOWN",
  "kpi": "KPI name or null",
  "dimension": "column name or null", 
  "filter": "filter value or null"
}}

EXAMPLES:

Query: "why did revenue drop?"
Output: {{"intent": "EXPLAIN_TREND", "kpi": "Revenue", "dimension": null, "filter": null}}

Query: "show by region"
Output: {{"intent": "SEGMENT_BY", "kpi": null, "dimension": "region", "filter": null}}

Query: "top 5 products"
Output: {{"intent": "TOP_N", "kpi": null, "dimension": "product", "filter": "5"}}

Query: "what happened last quarter?"
Output: {{"intent": "FILTER", "kpi": null, "dimension": null, "filter": "Q3"}}

Query: "gibberish xyz"
Output: {{"intent": "UNKNOWN", "kpi": null, "dimension": null, "filter": null}}

Now parse this query:"""

    return prompt


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
        return {"intent": "UNKNOWN", "kpi": None, "dimension": None, "filter": None}

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
            return {"intent": "UNKNOWN", "kpi": None, "dimension": None, "filter": None}

        # Parse JSON response
        # Handle cases where LLM adds markdown or explanation
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
        intent = json.loads(response_text)

        # Ensure all required fields present
        return {
            "intent": intent.get("intent", "UNKNOWN"),
            "kpi": intent.get("kpi") or None,
            "dimension": intent.get("dimension") or None,
            "filter": intent.get("filter") or None,
        }

    except json.JSONDecodeError as e:
        print(f"[INTENT_PARSER] JSON parse error: {e}")
        print(f"[INTENT_PARSER] Raw response: {response if response else 'None'}")
        return {"intent": "UNKNOWN", "kpi": None, "dimension": None, "filter": None}
    except Exception as e:
        print(f"[INTENT_PARSER] Error: {e}")
        return {"intent": "UNKNOWN", "kpi": None, "dimension": None, "filter": None}
