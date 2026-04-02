"""
Dashboard Plan Data Contracts
Phase 0B - Dataset Intelligence
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any


@dataclass(frozen=True)
class KPI:
    """KPI specification from LLM selection"""

    name: str
    source_column: Optional[str]
    aggregation: str  # sum, count, nunique
    segment_by: Optional[str] = None
    time_column: Optional[str] = None
    business_meaning: str = ""
    confidence: float = 0.0


@dataclass(frozen=True)
class ChartPlan:
    """Chart specification for dashboard"""

    chart_type: str  # line, bar, histogram
    title: str
    x_column: str
    y_column: Optional[str]
    kpi_name: str
    aggregation: Optional[str] = None
    segment_by: Optional[str] = None


@dataclass(frozen=True)
class DashboardPlan:
    """Complete dashboard plan from Phase 0B"""

    session_id: str
    kpis: List[KPI]
    charts: List[ChartPlan]
    story_arc: str
    kpi_coverage: float
    created_at: str
    kpi_candidates: List[Dict]  # ALL KPI candidates (not just selected)
