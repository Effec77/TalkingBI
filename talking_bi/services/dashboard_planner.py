"""
Dashboard Planner - Phase 0B.5
Generates chart specifications and story arc
"""

from typing import List, Dict
from datetime import datetime
from models.dashboard import KPI, ChartPlan, DashboardPlan


def create_dashboard_plan(
    session_id: str,
    kpis: List[Dict],
    dataset_context: Dict,
    kpi_candidates: List[Dict] = None,
) -> DashboardPlan:
    """
    Create a complete dashboard plan from validated KPIs.

    Args:
        session_id: Session identifier
        kpis: Validated KPIs
        dataset_context: Dataset metadata

    Returns:
        DashboardPlan with charts and story arc
    """
    print(f"[PLANNER] Creating dashboard plan for {len(kpis)} KPIs")

    # Convert dict KPIs to KPI objects
    kpi_objects = []
    for kpi_dict in kpis:
        kpi_obj = KPI(
            name=kpi_dict["name"],
            source_column=kpi_dict["source_column"],
            aggregation=kpi_dict["aggregation"],
            segment_by=kpi_dict.get("segment_by"),
            time_column=kpi_dict.get("time_column"),
            business_meaning=kpi_dict.get("business_meaning", ""),
            confidence=kpi_dict.get("confidence", 0.0),
        )
        kpi_objects.append(kpi_obj)

    # Generate charts for each KPI
    charts = []
    for kpi in kpi_objects:
        chart = _generate_chart_for_kpi(kpi, dataset_context)
        charts.append(chart)

    # Generate story arc
    story_arc = _generate_story_arc(kpi_objects, dataset_context)

    # Calculate KPI coverage
    kpi_coverage = len(kpi_objects) / 3.0  # We always aim for 3 KPIs

    # Store ALL KPI candidates for intent validation (not just selected)
    if kpi_candidates is None:
        kpi_candidates = []

    plan = DashboardPlan(
        session_id=session_id,
        kpis=kpi_objects,
        charts=charts,
        story_arc=story_arc,
        kpi_coverage=kpi_coverage,
        created_at=datetime.now().isoformat(),
        kpi_candidates=kpi_candidates,
    )

    print(
        f"[PLANNER] Created plan with {len(charts)} charts, coverage={kpi_coverage:.1%}"
    )

    return plan


def _generate_chart_for_kpi(kpi: KPI, dataset_context: Dict) -> ChartPlan:
    """
    Generate appropriate chart for a KPI.

    Rules:
    - If time_column exists → line chart
    - If segment_by exists → bar chart
    - Otherwise → bar chart (default)
    """

    categorical_cols = dataset_context.get("categorical_columns", [])
    datetime_cols = dataset_context.get("datetime_columns", [])

    # Determine chart type and axes.
    if kpi.time_column or datetime_cols:
        chart_type = "line"
        x_column = kpi.time_column or datetime_cols[0]
        y_column = "count" if kpi.aggregation == "count" else kpi.source_column
        title = f"{kpi.name} Over Time"
    elif kpi.segment_by or categorical_cols:
        chart_type = "bar"
        x_column = kpi.segment_by or categorical_cols[0]
        y_column = "count" if kpi.aggregation == "count" else kpi.source_column
        title = f"{kpi.name} by {x_column.replace('_', ' ').title()}"
    else:
        chart_type = "histogram"
        x_column = kpi.source_column or "record_index"
        y_column = "count" if kpi.aggregation == "count" else kpi.source_column
        title = f"{kpi.name} Distribution"

    assert x_column is not None
    assert y_column is not None

    chart = ChartPlan(
        chart_type=chart_type,
        title=title,
        x_column=x_column,
        y_column=y_column,
        kpi_name=kpi.name,
        aggregation=kpi.aggregation,
        segment_by=kpi.segment_by,
    )

    return chart


def _generate_story_arc(kpis: List[KPI], context: Dict) -> str:
    """
    Generate a narrative story arc for the dashboard.

    Simple template-based approach for Phase 0B.
    """

    segments = list(set([kpi.segment_by for kpi in kpis if kpi.segment_by]))

    story = f"""
Trends in {", ".join([k.name for k in kpis])} show measurable changes over time.

Segmentation by {", ".join(segments) if segments else "available business dimensions"} highlights performance differences across groups.

These patterns indicate key drivers of business performance and potential optimization areas.
""".strip()

    return story
