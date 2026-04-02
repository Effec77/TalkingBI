"""
Phase 6G: Deterministic Intent Override (Context-Aware)

Handles simple, common BI query patterns BEFORE LLM parsing.
Eliminates unnecessary LLM dependency for predictable queries.

Rules:
- NO semantic inference
- Pattern matching only
- Validate against schema
- Context-aware (uses conversation history)
- Log all decisions
"""

import re
from typing import Dict, Optional, Any


class DeterministicIntentDetector:
    """
    Context-aware deterministic intent detector.

    Uses conversation context to resolve ambiguous queries.
    """

    def __init__(self, schema_mapper, context_history=None):
        self.schema_mapper = schema_mapper
        self.context_history = context_history or []
        self.applied = False
        self.reason = None

    def has_context_kpi(self) -> bool:
        """Check if conversation context has a resolved KPI."""
        if not self.context_history:
            return False

        # Get last resolved intent
        last_run = self.context_history[-1]
        if isinstance(last_run, dict):
            intent = last_run.get("intent", {})
            if intent and intent.get("kpi"):
                return True

        return False

    def get_context_kpi(self) -> Optional[str]:
        """Get KPI from conversation context."""
        if not self.context_history:
            return None

        last_run = self.context_history[-1]
        if isinstance(last_run, dict):
            intent = last_run.get("intent", {})
            if intent:
                return intent.get("kpi")

        return None

    def detect_simple_segment(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Detect simple SEGMENT_BY patterns.

        Patterns:
        1. "by <dimension>" - always valid with context
        2. Just dimension name - valid if context has KPI
        """
        if not query:
            return None

        query_stripped = query.strip().lower()

        # Pattern 1: "by <dimension>" - ALWAYS triggers 6G
        if query_stripped.startswith("by "):
            dimension = query_stripped[3:].strip()
            mapped_dim, source = self.schema_mapper.map_dimension(dimension)

            if mapped_dim:
                self.applied = True
                self.reason = f"prefix_match: by {dimension}"
                print(f"[6G] Applied: True")
                print(f"[6G] Reason: {self.reason}")
                print(
                    f"[6G] Dimension mapped: '{dimension}' -> '{mapped_dim}' ({source})"
                )
                print(f"[6G] Skipped LLM parsing")

                # Inherit KPI from context if available
                context_kpi = self.get_context_kpi()

                return {
                    "intent": "SEGMENT_BY",
                    "kpi": context_kpi,  # Will be resolved by 6C if None
                    "kpi_1": None,
                    "kpi_2": None,
                    "dimension": mapped_dim,
                    "filter": None,
                    "source": "6G_deterministic",
                    "mapping_source": source,
                }
            else:
                print(f"[6G] Rejected: 'by {dimension}' - dimension not in schema")
                self.applied = False
                self.reason = "dimension_not_in_schema"
                return None

        # Pattern 2: Just a dimension name - only valid WITH context
        if " " not in query_stripped:
            mapped_dim, source = self.schema_mapper.map_dimension(query_stripped)

            if mapped_dim and source in ["exact_match", "normalized_match"]:
                # Require context KPI for standalone dimension
                if self.has_context_kpi():
                    self.applied = True
                    self.reason = f"dimension_match_with_context: {query_stripped}"
                    context_kpi = self.get_context_kpi()

                    print(f"[6G] Applied: True")
                    print(f"[6G] Reason: {self.reason}")
                    print(f"[6G] Dimension: '{query_stripped}' -> '{mapped_dim}'")
                    print(f"[6G] Context KPI: {context_kpi}")
                    print(f"[6G] Skipped LLM parsing")

                    return {
                        "intent": "SEGMENT_BY",
                        "kpi": context_kpi,
                        "kpi_1": None,
                        "kpi_2": None,
                        "dimension": mapped_dim,
                        "filter": None,
                        "source": "6G_deterministic",
                        "mapping_source": source,
                    }
                else:
                    print(
                        f"[6G] Skipped: '{query_stripped}' - no context KPI available"
                    )
                    self.applied = False
                    self.reason = "no_context_kpi"

        return None

    def detect_simple_filter(self, query: str) -> Optional[Dict[str, Any]]:
        """Detect simple FILTER patterns."""
        if not query:
            return None

        query_stripped = query.strip().lower()

        # Pattern: "filter <value>"
        if query_stripped.startswith("filter "):
            filter_value = query_stripped[7:].strip()
            self.applied = True
            self.reason = f"filter_prefix: {filter_value}"

            print(f"[6G] Applied: True")
            print(f"[6G] Reason: {self.reason}")
            print(f"[6G] Skipped LLM parsing")

            return {
                "intent": "FILTER",
                "kpi": None,
                "kpi_1": None,
                "kpi_2": None,
                "dimension": None,
                "filter": filter_value,
                "source": "6G_deterministic",
            }

        # Pattern: "<value> only"
        if query_stripped.endswith(" only"):
            filter_value = query_stripped[:-5].strip()
            self.applied = True
            self.reason = f"only_suffix: {filter_value}"

            print(f"[6G] Applied: True")
            print(f"[6G] Reason: {self.reason}")
            print(f"[6G] Skipped LLM parsing")

            return {
                "intent": "FILTER",
                "kpi": None,
                "kpi_1": None,
                "kpi_2": None,
                "dimension": None,
                "filter": filter_value,
                "source": "6G_deterministic",
            }

        return None

    def detect_simple_show(self, query: str) -> Optional[Dict[str, Any]]:
        """Detect simple "show <kpi>" patterns."""
        if not query:
            return None

        query_stripped = query.strip().lower()

        if query_stripped.startswith("show "):
            kpi_candidate = query_stripped[5:].strip()
            mapped_kpi, source = self.schema_mapper.map_kpi(kpi_candidate)

            if mapped_kpi:
                self.applied = True
                self.reason = f"show_kpi: {kpi_candidate}"

                print(f"[6G] Applied: True")
                print(f"[6G] Reason: {self.reason}")
                print(f"[6G] KPI mapped: '{kpi_candidate}' -> '{mapped_kpi}'")
                print(f"[6G] Skipped LLM parsing")

                return {
                    "intent": "SEGMENT_BY",
                    "kpi": mapped_kpi,
                    "kpi_1": None,
                    "kpi_2": None,
                    "dimension": None,
                    "filter": None,
                    "source": "6G_deterministic",
                    "mapping_source": source,
                }

        return None

    def detect(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Main entry point for Phase 6G detection.

        Returns:
            Intent dict if matched, None otherwise
        """
        # Try patterns in order
        intent = self.detect_simple_segment(query)
        if intent:
            return intent

        intent = self.detect_simple_filter(query)
        if intent:
            return intent

        intent = self.detect_simple_show(query)
        if intent:
            return intent

        # No match
        self.applied = False
        self.reason = "no_pattern_match"
        print(f"[6G] Applied: False")
        print(f"[6G] Reason: {self.reason}")
        return None


# Backward-compatible function
def apply_deterministic_override(
    query: str, schema_mapper, context_history=None
) -> Optional[Dict[str, Any]]:
    """
    Legacy entry point for Phase 6G.

    Args:
        query: User query
        schema_mapper: SchemaMapper instance
        context_history: Optional conversation history

    Returns:
        Intent dict if matched, None otherwise
    """
    detector = DeterministicIntentDetector(schema_mapper, context_history)
    return detector.detect(query)
