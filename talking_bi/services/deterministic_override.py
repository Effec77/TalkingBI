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
from typing import Dict, Optional, Any, List


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
                # kpi_source tracks WHERE the KPI came from (context vs query)
                kpi_source = "context" if context_kpi else None

                return {
                    "intent": "SEGMENT_BY",
                    "kpi": context_kpi,  # Will be resolved by 6C if None
                    "kpi_1": None,
                    "kpi_2": None,
                    "dimension": mapped_dim,
                    "filter": None,
                    "source": "6G_deterministic",
                    "mapping_source": source,
                    "kpi_source": kpi_source,  # 9C.2: provenance tracking
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
                        "kpi_source": "context",  # 9C.2: KPI inherited from context
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
            # Let parser handle multi-part queries like:
            # "show salary by department", "show salary and attrition", etc.
            blocker_tokens = [
                " by ",
                " and ",
                " with ",
                " vs ",
                " versus ",
                " against ",
                " over time",
            ]
            if any(tok in f" {kpi_candidate} " for tok in blocker_tokens):
                return None

            mapped_kpi, source = self.schema_mapper.map_kpi(kpi_candidate)

            # Only accept single resolved KPI names.
            if isinstance(mapped_kpi, str):
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

    def detect_show_by(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Detect deterministic pattern: "show <kpi> by <dimension>".
        Handles common modifiers like "average"/"total" in KPI phrase.
        """
        if not query:
            return None

        q = query.strip().lower()
        m = re.match(r"^show\s+(.+?)\s+by\s+(.+)$", q)
        if not m:
            return None

        kpi_phrase = m.group(1).strip()
        dim_phrase = m.group(2).strip()

        for prefix in ["average ", "avg ", "mean ", "total "]:
            if kpi_phrase.startswith(prefix):
                kpi_phrase = kpi_phrase[len(prefix) :].strip()
                break

        # High-confidence aliases for common HR/business terms.
        kpi_aliases = {
            "compensation": "salary",
            "pay": "salary",
            "wages": "salary",
            "attrition": "attrition_flag",
            "performance": "performance_score",
        }
        kpi_phrase = kpi_aliases.get(kpi_phrase, kpi_phrase)

        if hasattr(self.schema_mapper, "columns") and kpi_phrase in self.schema_mapper.columns:
            mapped_kpi, kpi_status = kpi_phrase, "resolved"
        else:
            mapped_kpi, kpi_status = self.schema_mapper.map_kpi(kpi_phrase)

        if hasattr(self.schema_mapper, "columns") and dim_phrase in self.schema_mapper.columns:
            mapped_dim, dim_status = dim_phrase, "resolved"
        else:
            mapped_dim, dim_status = self.schema_mapper.map_dimension(dim_phrase)

        if (
            kpi_status == "resolved"
            and dim_status == "resolved"
            and isinstance(mapped_kpi, str)
            and isinstance(mapped_dim, str)
        ):
            self.applied = True
            self.reason = f"show_by: {kpi_phrase} by {dim_phrase}"
            print(f"[6G] Applied: True")
            print(f"[6G] Reason: {self.reason}")
            print(f"[6G] Skipped LLM parsing")
            return {
                "intent": "SEGMENT_BY",
                "kpi": mapped_kpi,
                "kpi_1": None,
                "kpi_2": None,
                "dimension": mapped_dim,
                "filter": None,
                "source": "6G_deterministic",
            }

        return None

    def _extract_second_kpi(self, query: str) -> Optional[str]:
        """Extract and map the term after 'with' in a compare query.
        Returns None if no valid schema mapping found (prevents wrong fuzzy matches).
        """
        m = re.search(r"\bwith\s+(\S+)", query.lower())
        if m:
            candidate = m.group(1)
            mapped, _ = self.schema_mapper.map_kpi(candidate)
            # Only return if schema mapper found a real column — don't fall
            # back to the raw candidate to avoid false kpi_1==kpi_2 collisions
            return mapped if mapped else None
        return None

    def detect_compare(self, query: str) -> Optional[Dict[str, Any]]:
        """Phase 9C.2: deterministic COMPARE extraction for common patterns."""
        q = query.strip().lower()
        if not any(tok in q for tok in ["compare", " vs ", " versus ", " against "]):
            return None

        kpi_1_raw = None
        kpi_2_raw = None
        dim_raw = None

        # Pattern 0: "<dimension> wise compare <kpi1> versus <kpi2>"
        m = re.search(
            r"^(.+?)\s+wise\s+compare\s+(.+?)\s+(?:with|vs|versus|against)\s+(.+)$",
            q,
        )
        if m:
            dim_raw = m.group(1).strip()
            kpi_1_raw = m.group(2).strip()
            kpi_2_raw = m.group(3).strip()

        # Pattern A: compare <kpi1> with|vs|versus|against <kpi2> [by <dim>]
        if not kpi_1_raw or not kpi_2_raw:
            m = re.search(
                r"compare\s+(.+?)\s+(?:with|vs|versus|against)\s+(.+?)(?:\s+by\s+(.+))?$",
                q,
            )
            if m:
                kpi_1_raw = m.group(1).strip()
                kpi_2_raw = m.group(2).strip()
                dim_raw = (m.group(3) or "").strip() or dim_raw

        # Pattern A2: plot/show <kpi1> vs <kpi2> [by <dim>]
        if not kpi_1_raw or not kpi_2_raw:
            m = re.search(
                r"(?:plot|show)\s+(.+?)\s+(?:vs|versus|against)\s+(.+?)(?:\s+by\s+(.+))?$",
                q,
            )
            if m:
                kpi_1_raw = m.group(1).strip()
                kpi_2_raw = m.group(2).strip()
                dim_raw = (m.group(3) or "").strip() or dim_raw

        if kpi_1_raw and kpi_2_raw and not dim_raw:
            # Carry dimension from "<dimension> wise" anywhere in query.
            m = re.search(r"\b([a-z_ ]+)\s+wise\b", q)
            if m:
                dim_raw = m.group(1).strip()

        if kpi_1_raw and kpi_2_raw and not dim_raw:
            # Retain previous behavior for "compare ... by ..."
            m = re.search(
                r"compare\s+(.+?)\s+(?:with|vs|versus|against)\s+(.+?)(?:\s+by\s+(.+))?$",
                q,
            )
            if m and m.group(3):
                dim_raw = m.group(3).strip()

        if not kpi_1_raw or not kpi_2_raw:
            # Pattern B: compare <kpi1> and <kpi2> [by <dim>]
            m = re.search(r"compare\s+(.+?)\s+and\s+(.+?)(?:\s+by\s+(.+))?$", q)
            if m:
                kpi_1_raw = m.group(1).strip()
                kpi_2_raw = m.group(2).strip()
                dim_raw = (m.group(3) or "").strip() or dim_raw

        if not kpi_1_raw or not kpi_2_raw:
            return None

        kpi_aliases = {
            "compensation": "salary",
            "pay": "salary",
            "wages": "salary",
            "attrition": "attrition_flag",
            "performance": "performance_score",
        }
        kpi_1_raw = kpi_aliases.get(kpi_1_raw, kpi_1_raw)
        kpi_2_raw = kpi_aliases.get(kpi_2_raw, kpi_2_raw)

        # Keep this block for backward compatibility in existing logs/trace expectations.
        m = re.search(
            r"compare\s+(.+?)\s+(?:with|vs|versus|against)\s+(.+?)(?:\s+by\s+(.+))?$",
            q,
        )
        if m and not (kpi_1_raw and kpi_2_raw):
            kpi_1_raw = m.group(1).strip()
            kpi_2_raw = m.group(2).strip()
            dim_raw = (m.group(3) or "").strip() or None

        if not kpi_1_raw or not kpi_2_raw:
            return None

        # Strip trailing directive phrases from KPI chunks.
        for noise in [" by displaying a graph", " as a graph", " in a graph", " graph"]:
            if kpi_2_raw.endswith(noise):
                kpi_2_raw = kpi_2_raw[: -len(noise)].strip()

        if hasattr(self.schema_mapper, "columns") and kpi_1_raw in self.schema_mapper.columns:
            mapped_1, status_1 = kpi_1_raw, "resolved"
        else:
            mapped_1, status_1 = self.schema_mapper.map_kpi(kpi_1_raw)

        if hasattr(self.schema_mapper, "columns") and kpi_2_raw in self.schema_mapper.columns:
            mapped_2, status_2 = kpi_2_raw, "resolved"
        else:
            mapped_2, status_2 = self.schema_mapper.map_kpi(kpi_2_raw)

        kpi_1 = mapped_1 if status_1 == "resolved" and isinstance(mapped_1, str) else None
        kpi_2 = mapped_2 if status_2 == "resolved" and isinstance(mapped_2, str) else None

        # If they didn't strongly map to explicit metrics, fall back to LLM to parse dimension vs dimension limits
        if not kpi_1 or not kpi_2 or kpi_1 == kpi_2:
            return None

        mapped_dim = None
        if dim_raw:
            for noise in [" by displaying a graph", " as a graph", " in a graph", " graph"]:
                if dim_raw.endswith(noise):
                    dim_raw = dim_raw[: -len(noise)].strip()
            mapped_dim, dim_status = self.schema_mapper.map_dimension(dim_raw)
            if dim_status != "resolved" or not isinstance(mapped_dim, str):
                mapped_dim = None

        self.applied = True
        self.reason = f"compare_override: {kpi_1} vs {kpi_2}"
        print(f"[6G] Applied: True")
        print(f"[6G] Reason: {self.reason}")
        print(f"[6G] Skipped LLM parsing")
        return {
            "intent": "COMPARE",
            "kpi": None,
            "kpi_1": kpi_1,
            "kpi_2": kpi_2,
            "dimension": mapped_dim,
            "filter": None,
            "source": "6G_deterministic",
        }

    def detect_over_time(self, query: str) -> Optional[Dict[str, Any]]:
        """Phase 9C.2: 'over time' queries are handled by the orchestrator
        trend-lock block (which does proper date-column detection via profiler).
        We deliberately return None here so that path fires correctly.
        """
        # Intentional pass-through — orchestrator handles 'over time' / 'trend'
        return None

    def detect_not_null(self, query: str) -> Optional[Dict[str, Any]]:
        """Phase 9C.2: 'filter X not null' → structured NOT_NULL filter."""
        q = query.strip().lower()
        if "not null" not in q:
            return None

        # Extract column between 'filter' and 'not null'
        m = re.search(r"filter\s+(\S+)\s+not null", q)
        col_term = m.group(1) if m else None

        mapped_col = None
        if col_term:
            mapped_col, _ = self.schema_mapper.map_dimension(col_term)

        self.applied = True
        self.reason = f"not_null_override: {col_term}"
        print(f"[6G] Applied: True")
        print(f"[6G] Reason: {self.reason}")
        print(f"[6G] Skipped LLM parsing")
        return {
            "intent": "FILTER",
            "kpi": None,
            "kpi_1": None,
            "kpi_2": None,
            "dimension": None,
            "filter": {
                "column": mapped_col or col_term,
                "operator": "NOT_NULL"
            },
            "source": "6G_deterministic",
        }

    def detect(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Main entry point for Phase 6G detection.

        Phase 9C.2 overrides run first (compare, over_time, not_null)
        then the existing simple patterns.

        Returns:
            Intent dict if matched, None otherwise
        """
        # Phase 9C.2: High-priority overrides
        intent = self.detect_not_null(query)
        if intent:
            return intent

        intent = self.detect_over_time(query)
        if intent:
            return intent

        intent = self.detect_compare(query)
        if intent:
            return intent

        intent = self.detect_show_by(query)
        if intent:
            return intent

        # Existing patterns
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
