from dataclasses import asdict
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from graph.df_registry import deregister_df, register_df
from graph.executor import run_pipeline
from services.intelligence_engine import generate_dashboard_plan
from services.session_manager import get_session

router = APIRouter()


def _serialize_query_results(query_results: list) -> list:
    """
    Strip pandas DataFrames from query_results before HTTP serialization.
    The raw DataFrame is only needed inside the graph — prepared_data already
    holds the converted records.  We keep everything else for debugging.
    """
    import pandas as pd

    clean = []
    for qr in query_results:
        entry = {k: v for k, v in qr.items() if k != "data"}
        # Summarise data shape instead of full DataFrame
        data = qr.get("data")
        if isinstance(data, pd.DataFrame):
            entry["data_shape"] = list(data.shape)
            entry["data_preview"] = data.head(3).to_dict(orient="records")
        elif data is not None:
            entry["data"] = data  # scalar — fine to keep
        clean.append(entry)
    return clean


@router.post("/run/{session_id}")
async def run_execution_pipeline(session_id: str):
    """
    Phase 4 execution endpoint.
    Loads session -> builds dashboard plan -> registers df -> runs adaptive pipeline.
    Returns query_results, prepared_data, transformed_data, insights,
    insight_summary (LLM narrative), chart_specs, execution_trace.
    """
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} not found or expired",
        )

    df = session["df"]
    metadata = session.get("metadata")
    if metadata is None:
        raise HTTPException(status_code=400, detail="Session metadata not found")

    try:
        plan = generate_dashboard_plan(
            session_id=session_id,
            df=df,
            uploaded_dataset=metadata,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build dashboard plan for run: {exc}",
        ) from exc

    run_id = str(uuid4())

    dataset = {
        "filename": metadata.filename,
        "columns": metadata.columns,
        "shape": list(metadata.shape),
        "dtypes": metadata.dtypes,
        "missing_pct": metadata.missing_pct,
    }

    initial_state = {
        "session_id": session_id,
        "dataset": dataset,
        "dashboard_plan": {
            **asdict(plan),
            "_meta": {
                "kpi_count": len(plan.kpis),
                "chart_count": len(plan.charts),
            },
        },
        # shared_context carries lightweight metadata only.
        # The actual DataFrame lives in graph.df_registry keyed by run_id.
        "shared_context": {
            "applied_filters": [],
            "run_id": run_id,
        },
        "query_results": [],
        "prepared_data": None,
        "insights": [],
        "chart_specs": [],
        # Phase 3 fields
        "transformed_data": None,
        "retry_flags": {},
        "execution_trace": [],
        # Phase 4 fields
        "insight_summary": None,
        # Control flags
        "is_refinement": False,
        "target_components": [],
        "retry_count": 0,
        "errors": [],
        "run_id": run_id,
        "parent_run_id": None,
    }

    # ── Register DataFrame so nodes can retrieve it by run_id ────────────
    register_df(run_id, df)

    try:
        result_state = run_pipeline(initial_state)
    finally:
        # Always free the DataFrame, even if the pipeline crashes
        deregister_df(run_id)

    # ── Serialize: strip raw DataFrames before FastAPI JSON response ──────
    response = {
        "session_id": result_state["session_id"],
        "run_id": result_state["run_id"],
        "errors": result_state.get("errors", []),
        "query_results": _serialize_query_results(
            result_state.get("query_results", [])
        ),
        "prepared_data": result_state.get("prepared_data") or [],
        "transformed_data": result_state.get("transformed_data") or [],
        "insights": result_state.get("insights") or [],
        "insight_summary": result_state.get("insight_summary"),
        "chart_specs": result_state.get("chart_specs") or [],
        "execution_trace": result_state.get("execution_trace", []),
        "_summary": {
            "kpis_executed": len(result_state.get("query_results", [])),
            "kpis_succeeded": sum(
                1
                for r in result_state.get("query_results", [])
                if r.get("status") in ("success", "retry_success")
            ),
            "kpis_retried": sum(
                1
                for r in result_state.get("query_results", [])
                if r.get("status") == "retry_success"
            ),
            "charts": len(result_state.get("chart_specs") or []),
            "insights": len(result_state.get("insights") or []),
        },
    }

    # ── PHASE 5 — UI-Ready Response Block ───────────────────────────────────
    # Add structured, ranked data optimized for frontend consumption

    # Get planned KPI names for validation
    planned_kpis = {
        k.get("name", "")
        for k in result_state.get("dashboard_plan", {}).get("kpis", [])
    }

    # FIX 2 — Filter low-importance KPIs from UI (importance >= 10)
    top_kpis = result_state.get("transformed_data") or []
    top_kpis = [
        k
        for k in top_kpis
        if (abs(k.get("max", 0) - k.get("min", 0)) * (k.get("points", 1) ** 0.5)) >= 10
    ]

    # FIX 5 — Clean UI KPI output (only planned KPIs)
    top_kpis = [k for k in top_kpis if k.get("kpi") in planned_kpis]

    # FIX 6 — Limit insights in UI to top 3
    top_insights = (result_state.get("insights") or [])[:3]

    # FIX 5 — Clean UI charts (only planned KPIs)
    ui_charts = result_state.get("chart_specs") or []
    ui_charts = [c for c in ui_charts if c.get("kpi") in planned_kpis]

    response["ui"] = {
        "summary": result_state.get("insight_summary"),
        "top_kpis": top_kpis,
        "top_insights": top_insights,
        "charts": ui_charts,
    }

    return response
