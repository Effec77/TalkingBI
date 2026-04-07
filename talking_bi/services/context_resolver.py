"""
Context Resolver - Phase 6C (PRODUCTION-GRADE)

Resolves incomplete intents using conversation context.
Strict rules-based resolution with no silent guessing.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum


class ResolutionStatus(Enum):
    RESOLVED = "RESOLVED"
    INCOMPLETE = "INCOMPLETE"
    AMBIGUOUS = "AMBIGUOUS"
    UNKNOWN = "UNKNOWN"


@dataclass
class StructuredWarning:
    """Structured warning for UI consumption."""

    type: str
    field: str
    value: Any
    message: str = ""


@dataclass
class ResolutionResult:
    """Result of context resolution."""

    status: str
    intent: Optional[Dict[str, Any]] = None
    source_map: Dict[str, str] = field(default_factory=dict)
    warnings: List[StructuredWarning] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    ambiguity: Optional[Dict[str, Any]] = None
    context_used: bool = False
    context_applied: bool = False


class ContextResolver:
    """
    Resolves intent fields using conversation context.

    Rules:
    1. Context = last RESOLVED intent only (max 3 turns back)
    2. Priority: USER INPUT > CONTEXT > FALLBACK
    3. Only fills: kpi, dimension, filter
    4. Intent type NEVER inferred from context
    5. Fallback only if KPI required AND no context AND user provided NO fields
    6. COMPARE intent has kpi_1 (USER > CONTEXT) and kpi_2 (USER ONLY)
    7. Dimension NOT inherited when user provides KPI explicitly
    8. UNKNOWN intent returns immediately with no resolution

    Production Features:
    1. KPI normalization (consistent casing)
    2. Intent-level validation (COMPARE kpi_1 != kpi_2)
    3. Empty context guards
    4. Structured warnings for UI
    """

    def __init__(self, kpi_candidates: List[str], ambiguity_map: Dict[str, List[str]]):
        self.kpi_candidates = kpi_candidates
        # FIX 1: Build canonical KPI map for normalization
        self._canonical_kpi_map = {k.lower(): k for k in kpi_candidates}
        self.ambiguity_map = ambiguity_map
        self.context_history: List[Dict[str, Any]] = []  # Last 3 resolved intents

    def add_to_context(self, resolved_intent: Dict[str, Any]):
        """Add a resolved intent to context history (max 3)."""
        self.context_history.append(resolved_intent)
        if len(self.context_history) > 3:
            self.context_history.pop(0)

    def get_last_resolved_context(self, current_columns: List[str] = None) -> Optional[Dict[str, Any]]:
        """Get last resolved intent from context (with dataset validation)."""
        if not self.context_history:
            return None
        context = self.context_history[-1]
        # Guard against empty context object
        if not context or not isinstance(context, dict):
            return None
        
        # Rule 3 Phase 9C: Dataset consistency check
        kpi = context.get("kpi")
        if not kpi:
            return None
            
        if current_columns is not None:
             # Case-insensitive column check
             col_lower = [c.lower() for c in current_columns]
             if kpi.lower() not in col_lower:
                 print(f"[6C] Discarding context KPI '{kpi}' - not in current dataset")
                 return None
                 
        return context

    def _normalize_kpi(self, kpi: str) -> str:
        """FIX 1: Normalize KPI to canonical form (proper casing)."""
        if not kpi:
            return kpi
        kpi_lower = kpi.lower()
        return self._canonical_kpi_map.get(kpi_lower, kpi)

    def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
        current_columns: List[str] = None,
    ) -> ResolutionResult:
        print(f"[TRACE:RESOLVER] Input: {parsed_intent}")
        if parsed_intent.get("_locked"):
            print(f"[TRACE:RESOLVER] LOCK ACTIVE: {parsed_intent.get('_lock_source')}")

        # Fix 1: Trend Lock Check (Phase 9C.1)
        if parsed_intent.get("_locked"):
            self.add_to_context(parsed_intent)
            return ResolutionResult(
                status=ResolutionStatus.RESOLVED.value,
                intent=parsed_intent.copy(),
                source_map={},
                warnings=[],
                missing_fields=[],
                context_used=False,
                context_applied=False,
            )

        """
        Resolve intent using context and fallback rules.

        Args:
            parsed_intent: Intent from parser
            dashboard_plan: Dashboard plan for fallback KPI

        Returns:
            ResolutionResult with status and resolved intent
        """
        intent_type = parsed_intent.get("intent", "UNKNOWN")

        # UNKNOWN intent - return immediately with no resolution
        if intent_type == "UNKNOWN":
            return ResolutionResult(
                status=ResolutionStatus.UNKNOWN.value,
                intent=None,
                source_map={},
                warnings=[],
                missing_fields=[],
                ambiguity=None,
                context_used=False,
            )

        kpi = parsed_intent.get("kpi")
        # Rule 3: Context carry for KPI
        context_applied = False
        if not kpi:
            last_context = self.get_last_resolved_context(current_columns)
            if last_context and last_context.get("kpi"):
                kpi = last_context["kpi"]
                context_applied = True
                print(f"[6C] Context applied: KPI={kpi}")
        dimension = parsed_intent.get("dimension")
        filter_val = parsed_intent.get("filter")

        # COMPARE intent handling
        # Note: Parser might populate 'kpi' which should be treated as kpi_1
        if intent_type == "COMPARE":
            # If kpi_1 not provided but kpi is, treat kpi as kpi_1
            if not parsed_intent.get("kpi_1") and parsed_intent.get("kpi"):
                parsed_intent["kpi_1"] = parsed_intent["kpi"]
            return self._resolve_compare(parsed_intent, dashboard_plan)

        # Build source map tracking
        source_map = {}
        warnings: List[StructuredWarning] = []
        missing_fields = []
        ambiguity = None

        # Check for ambiguity in KPI (only if not an exact KPI candidate match)
        if kpi and not self._is_kpi_candidate(kpi) and self._is_ambiguous(kpi):
            ambiguous_terms = self._get_ambiguous_terms(kpi)
            # Clean ambiguity handling - no warning, just structured data
            return ResolutionResult(
                status=ResolutionStatus.AMBIGUOUS.value,
                intent=None,
                source_map={},
                warnings=[],
                missing_fields=[],
                ambiguity={"field": "kpi", "term": kpi, "options": ambiguous_terms},
                context_used=False,
            )

        # Track what user actually provided (not from context/fallback)
        user_provided_fields = []
        if kpi:
            user_provided_fields.append("kpi")
        if dimension:
            user_provided_fields.append("dimension")
        if filter_val:
            user_provided_fields.append("filter")

        # Resolve KPI
        resolved_kpi, kpi_source = self._resolve_kpi(
            kpi, intent_type, dashboard_plan, user_provided_fields
        )
        if resolved_kpi:
            # FIX 1: Normalize KPI name
            resolved_kpi = self._normalize_kpi(resolved_kpi)
            source_map["kpi"] = kpi_source
            if kpi_source == "fallback":
                # FIX 4: Structured fallback warning
                warnings.append(
                    StructuredWarning(
                        type="fallback_used",
                        field="kpi",
                        value=resolved_kpi,
                        message="kpi inferred from dashboard default",
                    )
                )
            elif kpi_source == "context":
                # FIX 4: Structured context warning
                warnings.append(
                    StructuredWarning(
                        type="context_inheritance",
                        field="kpi",
                        value=resolved_kpi,
                        message=f"KPI inherited from context: {resolved_kpi}",
                    )
                )
        else:
            missing_fields.append("kpi")

        # Dimension over-inheritance - don't inherit when user provides KPI
        user_provided_kpi = kpi is not None
        resolved_dimension, dim_source = self._resolve_dimension(
            dimension, user_provided_kpi
        )
        if resolved_dimension:
            source_map["dimension"] = dim_source
            if dim_source == "context":
                # FIX 4: Structured context warning
                warnings.append(
                    StructuredWarning(
                        type="context_inheritance",
                        field="dimension",
                        value=resolved_dimension,
                        message=f"Dimension inherited from context: {resolved_dimension}",
                    )
                )

        # Resolve filter
        resolved_filter, filter_source = self._resolve_filter(filter_val)
        if resolved_filter:
            source_map["filter"] = filter_source
            if filter_source == "context":
                # FIX 4: Structured context warning
                warnings.append(
                    StructuredWarning(
                        type="context_inheritance",
                        field="filter",
                        value=resolved_filter,
                        message=f"Filter inherited from context: {resolved_filter}",
                    )
                )

        # Build resolved intent
        resolved_intent = {
            "intent": intent_type,
            "kpi": resolved_kpi,
            "dimension": resolved_dimension,
            "filter": resolved_filter,
        }

        # Calculate context_used flag
        context_used = any(v == "context" for v in source_map.values())

        # Determine status
        if missing_fields:
            return ResolutionResult(
                status=ResolutionStatus.INCOMPLETE.value,
                intent=resolved_intent,
                source_map=source_map,
                warnings=warnings,
                missing_fields=missing_fields,
                ambiguity=None,
                context_used=context_used, context_applied=context_applied,
            )

        # Success - add to context for future turns
        self.add_to_context(resolved_intent)

        return ResolutionResult(
            status=ResolutionStatus.RESOLVED.value,
            intent=resolved_intent,
            source_map=source_map,
            warnings=warnings,
            missing_fields=[],
            ambiguity=None,
            context_used=context_used, context_applied=context_applied,
        )

    def _resolve_compare(
        self, parsed_intent: Dict[str, Any], dashboard_plan: Optional[Dict[str, Any]]
    ) -> ResolutionResult:
        print(f"[TRACE:RESOLVER:COMPARE] Input: {parsed_intent}")

        """
        Resolve COMPARE intent with kpi_1 and kpi_2.

        Rules:
        - kpi_1: USER > CONTEXT
        - kpi_2: USER ONLY (NEVER from context)
        - FIX 2: kpi_1 != kpi_2 (validation)
        """
        # Prefer explicit kpi_1 for compare, fallback to legacy "kpi" field.
        kpi_1 = parsed_intent.get("kpi_1") or parsed_intent.get("kpi")
        kpi_2 = parsed_intent.get("kpi_2")
        dimension = parsed_intent.get("dimension")
        filter_val = parsed_intent.get("filter")

        source_map = {}
        warnings: List[StructuredWarning] = []
        missing_fields = []
        context_applied = False

        # Resolve kpi_1 (USER > CONTEXT) - Fix 3 Phase 9C.1
        resolved_kpi_1 = None
        kpi_1_source = None
        if kpi_1:
            # FIX 1: Normalize KPI
            resolved_kpi_1 = self._normalize_kpi(kpi_1)
            kpi_1_source = "user"
            source_map["kpi_1"] = "user"
        else:
            # Rule 3 Phase 9C.1: Compare context inheritance
            print(f"[6C] COMPARE intent missing kpi_1 - searching context")
            # Try context
            context = self.get_last_resolved_context()
            if context and context.get("kpi"):
                resolved_kpi_1 = self._normalize_kpi(context["kpi"])
                kpi_1_source = "context"
                source_map["kpi_1"] = "context"
                context_applied = True
                warnings.append(
                    StructuredWarning(
                        type="context_inheritance",
                        field="kpi_1",
                        value=resolved_kpi_1,
                        message=f"KPI-1 inherited from context: {resolved_kpi_1}",
                    )
                )
            else:
                missing_fields.append("kpi_1")

        # Resolve kpi_2 (USER ONLY)
        resolved_kpi_2 = None
        if kpi_2:
            # FIX 1: Normalize KPI
            resolved_kpi_2 = self._normalize_kpi(kpi_2)
            source_map["kpi_2"] = "user"
        else:
            missing_fields.append("kpi_2")

        # FIX 2: Intent-level validation - kpi_1 must be different from kpi_2
        if resolved_kpi_1 and resolved_kpi_2:
            if resolved_kpi_1.lower() == resolved_kpi_2.lower():
                return ResolutionResult(
                    status=ResolutionStatus.INCOMPLETE.value,
                    intent={
                        "intent": "COMPARE",
                        "kpi_1": resolved_kpi_1,
                        "kpi_2": resolved_kpi_2,
                    },
                    source_map=source_map,
                    warnings=[
                        StructuredWarning(
                            type="validation_error",
                            field="kpi_1,kpi_2",
                            value=None,
                            message="kpi_1 and kpi_2 must be different KPIs",
                        )
                    ],
                    missing_fields=["kpi_2"],  # kpi_2 is effectively invalid
                    ambiguity=None,
                    context_used=False,
                )

        # Resolve dimension (USER > CONTEXT)
        resolved_dimension = None
        dim_source = None
        if dimension:
            resolved_dimension = dimension
            dim_source = "user"
            source_map["dimension"] = "user"
        else:
            context = self.get_last_resolved_context()
            if context and context.get("dimension"):
                resolved_dimension = context["dimension"]
                dim_source = "context"
                source_map["dimension"] = "context"
                warnings.append(
                    StructuredWarning(
                        type="context_inheritance",
                        field="dimension",
                        value=resolved_dimension,
                        message=f"Dimension inherited from context: {resolved_dimension}",
                    )
                )

        # Resolve filter
        resolved_filter = None
        if filter_val:
            resolved_filter = filter_val
            source_map["filter"] = "user"

        # Build resolved intent
        resolved_intent = {
            "intent": "COMPARE",
            "kpi_1": resolved_kpi_1,
            "kpi_2": resolved_kpi_2,
            "dimension": resolved_dimension,
            "filter": resolved_filter,
        }

        # Calculate context_used
        context_used = any(v == "context" for v in source_map.values())

        # Determine status
        if missing_fields:
            return ResolutionResult(
                status=ResolutionStatus.INCOMPLETE.value,
                intent=resolved_intent,
                source_map=source_map,
                warnings=warnings,
                missing_fields=missing_fields,
                ambiguity=None,
                context_used=context_used, context_applied=context_applied,
            )

        # Success - add to context
        self.add_to_context(resolved_intent)

        return ResolutionResult(
            status=ResolutionStatus.RESOLVED.value,
            intent=resolved_intent,
            source_map=source_map,
            warnings=warnings,
            missing_fields=[],
            ambiguity=None,
            context_used=context_used, context_applied=context_applied,
        )

    def _is_ambiguous(self, term: str) -> bool:
        """Check if term is in ambiguity map (case-sensitive)."""
        return term in self.ambiguity_map.keys()

    def _get_ambiguous_terms(self, term: str) -> List[str]:
        """Get ambiguous options for a term (case-sensitive)."""
        return self.ambiguity_map.get(term, [])

    def _is_kpi_candidate(self, term: str) -> bool:
        """Check if term is a valid KPI candidate (case-sensitive)."""
        return term in self.kpi_candidates

    def _resolve_kpi(
        self,
        kpi: Optional[str],
        intent_type: str,
        dashboard_plan: Optional[Dict[str, Any]],
        user_provided_fields: list,
    ) -> tuple:
        """
        Resolve KPI using priority: USER > CONTEXT > FALLBACK.

        Fallback only when:
        1. KPI is missing
        2. KPI is required by intent
        3. NO valid context exists
        4. Intent != UNKNOWN

        Returns:
            (resolved_kpi, source) or (None, None)
        """
        # Priority 1: User provided
        if kpi:
            return kpi, "user"

        # Priority 2: Context (if applicable)
        context = self.get_last_resolved_context()
        if context and context.get("kpi"):
            # Only inherit KPI for certain intent types
            if intent_type in [
                "SEGMENT_BY",
                "FILTER",
                "COMPARE",
                "EXPLAIN_TREND",
                "EXPLAIN_ANOMALY",
            ]:
                return context["kpi"], "context"

        # Priority 3: Fallback
        # Only fires when: KPI required, no context, AND user provided ZERO fields.
        # If user gave dimension but no KPI → INCOMPLETE (partial query, be explicit).
        # Example: "show trends"          → fallback fires (bare query, no fields)
        # Example: "show trends by region" → INCOMPLETE (dimension provided, KPI missing)
        if intent_type in ["SEGMENT_BY", "FILTER", "EXPLAIN_TREND", "EXPLAIN_ANOMALY"]:
            if not context and not user_provided_fields:
                if dashboard_plan and dashboard_plan.get("kpis"):
                    fallback_kpi = dashboard_plan["kpis"][0]
                    if isinstance(fallback_kpi, dict):
                        fallback_kpi = fallback_kpi.get("name", "Sales")
                    return fallback_kpi, "fallback"

        return None, None

    def _resolve_dimension(
        self, dimension: Optional[str], user_provided_kpi: bool = False
    ) -> tuple:
        """
        Resolve dimension using priority: USER > CONTEXT.

        Dimension over-inheritance - don't inherit when user provides KPI
        """
        # Priority 1: User provided
        if dimension:
            return dimension, "user"

        # Don't inherit from context if user explicitly provided KPI
        if user_provided_kpi:
            return None, None

        # Priority 2: Context
        context = self.get_last_resolved_context()
        if context and context.get("dimension"):
            return context["dimension"], "context"

        return None, None

    def _resolve_filter(self, filter_val: Optional[str]) -> tuple:
        """Resolve filter using priority: USER > CONTEXT."""
        # Priority 1: User provided
        if filter_val:
            return filter_val, "user"

        # Priority 2: Context
        context = self.get_last_resolved_context()
        if context and context.get("filter"):
            return context["filter"], "context"

        return None, None


def create_resolver(
    kpi_candidates: List[str], ambiguity_map: Optional[Dict[str, List[str]]] = None
):
    """Factory function to create a context resolver."""
    if ambiguity_map is None:
        ambiguity_map = {
            "sales": ["gross_sales", "net_sales"],
            "profit": ["gross_profit", "net_profit"],
        }
    return ContextResolver(kpi_candidates, ambiguity_map)


# FIX 5: Context Expiry Note
# Future enhancement for Phase 6D/7:
# - Add context_ttl: int (number of turns before context expires)
# - Add timestamp tracking to each context entry
# - In get_last_resolved_context(), filter out entries older than context_ttl
# Example:
#   def is_context_fresh(self, entry: Dict) -> bool:
#       age = current_turn - entry.get("turn", 0)
#       return age <= self.context_ttl
