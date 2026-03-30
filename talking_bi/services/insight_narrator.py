"""
Insight Narrator — Phase 4: Analytical Intelligence

LLM enhancement layer for insights. The LLM NEVER computes metrics.
It receives only pre-validated, structured insight objects and converts
them into a human-readable business narrative.

Rules:
  - LLM does NOT access raw DataFrame
  - LLM does NOT compute numbers
  - LLM only reformats what it receives
  - If LLM fails, the system still works (insights remain structured)
"""
import json
from typing import Optional


class InsightNarrator:
    """
    Converts structured insight dicts into a concise business narrative.
    Uses the LLMManager fallback chain. Gracefully returns None on failure.
    """

    def __init__(self, llm_manager):
        self.llm = llm_manager

    def generate(self, insights: list) -> Optional[str]:
        """
        Generate a narrative summary from structured insights.

        Args:
            insights: list of validated insight dicts from insight_node

        Returns:
            A string narrative, or None if LLM is unavailable.
        """
        if not insights:
            return None

        # Sanitise: only pass serialisable fields to the LLM
        safe_insights = []
        for ins in insights:
            safe_insights.append({
                "kpi": ins.get("kpi", "unknown"),
                "type": ins.get("type", "unknown"),
                "details": ins.get("details", {}),
                "confidence": ins.get("confidence", 0),
            })

        prompt = f"""You are a business intelligence analyst.
Convert the following structured data insights into a clear, concise business summary.

STRICT RULES:
- DO NOT invent numbers or statistics
- DO NOT add information not present in the data
- Use ONLY the numbers and facts provided below
- Keep the summary to 2-4 sentences maximum
- Write in professional, clear language

Structured Insights:
{json.dumps(safe_insights, indent=2)}

Business Summary:"""

        try:
            response = self.llm.call_llm(
                prompt=prompt,
                cache_key=f"narrative_{hash(json.dumps(safe_insights, sort_keys=True))}",
            )

            if response:
                # Clean up the response — strip whitespace, remove markdown artifacts
                summary = response.strip()
                # Remove any markdown code fences the LLM might add
                if summary.startswith("```"):
                    lines = summary.split("\n")
                    summary = "\n".join(
                        l for l in lines if not l.strip().startswith("```")
                    ).strip()
                print(f"[NARRATOR] Generated summary ({len(summary)} chars)")
                return summary

        except Exception as e:
            print(f"[NARRATOR] LLM call failed: {e}")

        return None
