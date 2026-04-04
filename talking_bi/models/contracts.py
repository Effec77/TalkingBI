"""
Phase 9 Output Contracts

Single canonical response format for the entire Talking BI system.
This ensures consistency across all API endpoints and execution paths.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass(frozen=True)
class UploadedDataset:
    """Metadata about an uploaded dataset."""

    session_id: str
    filename: str
    columns: List[str]
    dtypes: Dict[str, str]
    shape: tuple
    sample_values: Dict[str, List[str]]
    missing_pct: Dict[str, float]


@dataclass
class OrchestratorResult:
    """
    Single system-wide response format.

    Used by:
    - QueryOrchestrator (primary producer)
    - API layer (serialization to JSON)
    - UI layer (consumption)
    - Evaluator (recording)
    """

    # Core status
    status: str  # RESOLVED | INCOMPLETE | UNKNOWN | AMBIGUOUS | INVALID | ERROR

    # Request context
    query: str
    session_id: str

    # Resolved intent (what the system understood)
    intent: Dict[str, Any]  # {intent_type, kpi, dimension, filter, ...}

    # Semantic interpretation metadata (Phase 7)
    semantic_meta: Dict[str, Any] = field(default_factory=dict)
    # Example: {"applied": True, "mapped_to": "revenue", "confidence": 0.85}

    # Data outputs
    data: List[Dict[str, Any]] = field(default_factory=list)  # prepared_data
    charts: List[Dict[str, Any]] = field(default_factory=list)  # chart_specs
    insights: List[Dict[str, Any]] = field(default_factory=list)  # insights

    # Execution plan metadata (Phase 6D)
    plan: Dict[str, Any] = field(default_factory=dict)
    # Example: {"mode": "PARTIAL_RUN", "reuse": "filtered_df", "operations": [...]}

    # Performance metrics
    latency_ms: float = 0.0

    # Quality indicators
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Full execution trace (for debugging and evaluation)
    trace: Dict[str, Any] = field(default_factory=dict)
    # Contains: normalized_query, 6g_applied, semantic_applied, mapped_fields, execution_path

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status,
            "query": self.query,
            "session_id": self.session_id,
            "intent": self.intent,
            "semantic_meta": self.semantic_meta,
            "data": self.data,
            "charts": self.charts,
            "insights": self.insights,
            "plan": self.plan,
            "latency_ms": self.latency_ms,
            "warnings": self.warnings,
            "errors": self.errors,
            "trace": self.trace,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrchestratorResult":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ExecutionTrace:
    """
    Detailed trace of query processing through all phases.
    """

    # Phase 6E: Normalization
    normalized_query: str = ""
    normalization_applied: bool = False
    normalization_changes: List[str] = field(default_factory=list)

    # Phase 6G: Deterministic Override
    g6_applied: bool = False
    g6_reason: str = ""  # e.g., "prefix_match: by region"

    # Phase 6B: Parsing
    parser_used: str = ""  # "deterministic" | "llm"
    raw_intent: Dict[str, Any] = field(default_factory=dict)

    # Phase 7: Semantic Interpretation
    semantic_applied: bool = False
    semantic_mapping: Optional[str] = None
    semantic_confidence: float = 0.0

    # Phase 6F: Schema Mapping
    mapped_fields: Dict[str, str] = field(default_factory=dict)
    # Example: {"kpi": "revenue", "dimension": "region"}

    # Phase 6C: Context Resolution
    context_used: bool = False
    context_kpi_inherited: Optional[str] = None

    # Phase 6D: Execution Planning
    execution_path: List[str] = field(default_factory=list)
    # Example: ["filter", "groupby", "aggregate", "chart"]

    # Phase 9B: Production Execution
    backend_used: str = "pandas"
    cache_hit: bool = False
    llm_cache_hit: bool = False
    fallback_triggered: bool = False

    # Fixes
    kpi_validation: Dict[str, Any] = field(default_factory=dict)
    cache_used: bool = False
    cache_reason: str = ""
    context_applied: bool = False
    kpi_resolution: Dict[str, Any] = field(default_factory=dict)
    trend_detected: bool = False
    trend_dimension: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "normalized_query": self.normalized_query,
            "normalization_applied": self.normalization_applied,
            "normalization_changes": self.normalization_changes,
            "6g_applied": self.g6_applied,
            "6g_reason": self.g6_reason,
            "parser_used": self.parser_used,
            "raw_intent": self.raw_intent,
            "semantic_applied": self.semantic_applied,
            "semantic_mapping": self.semantic_mapping,
            "semantic_confidence": self.semantic_confidence,
            "mapped_fields": self.mapped_fields,
            "context_used": self.context_used,
            "context_kpi_inherited": self.context_kpi_inherited,
            "execution_path": self.execution_path,
            "backend_used": self.backend_used,
            "cache_hit": self.cache_hit,
            "llm_cache_hit": self.llm_cache_hit,
            "fallback_triggered": self.fallback_triggered,
            "kpi_validation": self.kpi_validation,
            "cache_used": self.cache_used,
            "cache_reason": self.cache_reason,
            "context_applied": self.context_applied,
            "kpi_resolution": self.kpi_resolution,
            "trend_detected": self.trend_detected,
            "trend_dimension": self.trend_dimension,
        }


# Legacy alias for backward compatibility during transition
OrchestratorResponse = OrchestratorResult
