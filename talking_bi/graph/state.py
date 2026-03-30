from typing import TypedDict, List, Optional


class PipelineState(TypedDict):
    session_id: str
    dataset: dict
    dashboard_plan: dict

    shared_context: dict

    # Phase 2 — execution outputs
    query_results: List[dict]
    prepared_data: Optional[list]
    insights: List[dict]
    chart_specs: List[dict]

    # Phase 4 — Analytical Intelligence
    insight_summary: Optional[str]     # LLM-generated narrative (enhancement only)

    # Phase 3 — DeepPrep + adaptive execution
    transformed_data: Optional[list]   # min/max/points per timeseries KPI
    retry_flags: dict                  # {kpi_name: bool} — tracks retry attempts
    execution_trace: list              # ordered list of visited node names

    is_refinement: bool
    target_components: List[str]

    retry_count: int
    errors: List[str]

    run_id: str
    parent_run_id: Optional[str]
