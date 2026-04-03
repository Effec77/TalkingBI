"""
Adaptive Executor — Phase 6D

The single authority that translates an ExecutionPlan into actual
computation. It is the ONLY caller of run_pipeline (LangGraph).

Rules:
  1. FULL_RUN  → delegates to run_pipeline (LangGraph, all nodes)
  2. PARTIAL_RUN → executes ONLY the plan.operations, reusing cached
                   DataFrames from ExecutionState
  3. Never bypasses the plan
  4. Never mutates prev_state
  5. Always returns AdaptiveResult with base_df, filtered_df, final_output
     plus the full pipeline result dict for the API response layer

Partial execution maps to existing node logic through helper
functions lifted directly from nodes.py / pandas — no LLM, no graph.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd

from graph.df_registry import deregister_df, register_df
from graph.executor import run_pipeline
from graph.state import PipelineState
from services.execution_planner import (
    ExecutionPlan,
    ExecutionState,
    REUSE_BASE_DF,
    REUSE_FILTERED_DF,
    REUSE_LAST_RESULT,
    STEP_FILTER,
    STEP_GROUPBY,
    STEP_AGGREGATE,
    STEP_COMPUTE_KPI_1,
    STEP_COMPUTE_KPI_2,
    STEP_RENDER,
    MODE_FULL,
    MODE_PARTIAL,
)


# ─────────────────────────────────────────────────────────────
# Result struct returned by adaptive_execute()
# ─────────────────────────────────────────────────────────────


@dataclass
class AdaptiveResult:
    """
    Unified result from any execution path (FULL or PARTIAL).

    The API layer reads from `pipeline_result` exactly as before —
    no callers need to change.  base_df / filtered_df / final_output
    are used by the StateStore to update ExecutionState.
    """

    # Cacheable artifacts (for next turn's ExecutionState)
    base_df: Optional[pd.DataFrame] = None
    filtered_df: Optional[pd.DataFrame] = None
    final_output: Optional[List[Dict[str, Any]]] = None  # prepared_data

    # Full pipeline response dict (same shape as run_pipeline result)
    pipeline_result: Dict[str, Any] = field(default_factory=dict)

    # Execution metadata
    mode_used: str = MODE_FULL
    operations_run: List[str] = field(default_factory=list)
    plan_reason: str = ""


# ─────────────────────────────────────────────────────────────
# Partial execution helpers (pure pandas, no LangGraph)
# ─────────────────────────────────────────────────────────────


def _apply_filter(df: pd.DataFrame, intent: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply intent.filter to base_df.

    Currently uses column-value equality matching.
    Filter format examples: "Q4", "region=West", "2023".

    FIX 2: Handles null/none/nan filters safely using isna().

    Returns filtered DataFrame (or original if no filter or parse fails).
    """
    filter_val = intent.get("filter")
    if not filter_val:
        return df.copy()

    # FIX 2: Handle null/none/nan values
    filter_str = str(filter_val).lower().strip()
    is_null_filter = filter_str in ["null", "none", "nan"]

    dimension = intent.get("dimension")

    # Try "column=value" format first
    if "=" in str(filter_val):
        parts = str(filter_val).split("=", 1)
        col, val = parts[0].strip(), parts[1].strip()
        if col in df.columns:
            # FIX 2: Handle null filter in column=value format
            val_lower = val.lower().strip()
            if val_lower in ["null", "none", "nan"]:
                filtered = df[df[col].isna()]
                print(f"[6D:filter] Applied {col}=NULL → {len(filtered)} rows")
                return filtered if not filtered.empty else df.copy()

            filtered = df[df[col].astype(str).str.strip() == val]
            print(f"[6D:filter] Applied {col}={val} → {len(filtered)} rows")
            return filtered if not filtered.empty else df.copy()

    # If dimension is known, try filtering on it
    if dimension and dimension in df.columns:
        # FIX 2: Handle null filter on dimension
        if is_null_filter:
            filtered = df[df[dimension].isna()]
            print(f"[6D:filter] Applied {dimension}=NULL → {len(filtered)} rows")
            return filtered if not filtered.empty else df.copy()

        filtered = df[
            df[dimension]
            .astype(str)
            .str.contains(str(filter_val), case=False, na=False)
        ]
        if not filtered.empty:
            print(
                f"[6D:filter] Applied dimension filter '{filter_val}' on '{dimension}' → {len(filtered)} rows"
            )
            return filtered

    # Fallback: string search across all string columns
    # FIX 2: Don't search for null in string columns, return unfiltered
    if is_null_filter:
        print(f"[6D:filter] Null filter without dimension — returning unfiltered")
        return df.copy()

    for col in df.select_dtypes(include="object").columns:
        filtered = df[
            df[col].astype(str).str.contains(str(filter_val), case=False, na=False)
        ]
        if not filtered.empty:
            print(
                f"[6D:filter] Applied fuzzy filter '{filter_val}' on '{col}' → {len(filtered)} rows"
            )
            return filtered

    print(f"[6D:filter] Could not apply filter '{filter_val}' — returning unfiltered")
    return df.copy()


def _apply_groupby_aggregate(
    df: pd.DataFrame,
    kpi_spec: Dict[str, Any],
    dimension: Optional[str],
) -> Any:
    """
    Apply groupby + aggregation for a single KPI spec.
    Returns DataFrame (grouped) or scalar.
    """
    col = kpi_spec.get("source_column")
    agg = (kpi_spec.get("aggregation") or "sum").lower()
    group_col = dimension or kpi_spec.get("time_column") or kpi_spec.get("segment_by")

    if col and col not in df.columns:
        raise ValueError(f"Column '{col}' not found for KPI '{kpi_spec.get('name')}'")

    if agg == "count":
        if group_col and group_col in df.columns:
            return df.groupby(group_col).size().reset_index(name="value")
        return len(df)

    if agg == "sum":
        if group_col and group_col in df.columns:
            return df.groupby(group_col)[col].sum().reset_index(name="value")
        return float(df[col].sum())

    if agg in ("avg", "mean"):
        if group_col and group_col in df.columns:
            return df.groupby(group_col)[col].mean().reset_index(name="value")
        return float(df[col].mean())

    if agg == "min":
        return float(df[col].min())

    if agg == "max":
        return float(df[col].max())

    if agg == "nunique":
        if group_col and group_col in df.columns:
            return df.groupby(group_col)[col].nunique().reset_index(name="value")
        return int(df[col].nunique())

    # Fallback
    return len(df)


def _build_prepared_data(
    kpi_specs: List[Dict[str, Any]],
    df: pd.DataFrame,
    dimension: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Compute prepared_data for a list of KPI specs.
    Returns the same structure as prep_node (list of dicts).
    """
    prepared = []
    for kpi in kpi_specs:
        kpi_name = kpi.get("name", "unknown")
        try:
            result = _apply_groupby_aggregate(df, kpi, dimension)
            if isinstance(result, pd.DataFrame):
                prepared.append(
                    {
                        "kpi": kpi_name,
                        "type": "timeseries",
                        "data": result.to_dict(orient="records"),
                    }
                )
            else:
                prepared.append(
                    {
                        "kpi": kpi_name,
                        "type": "scalar",
                        "value": result,
                    }
                )
            print(f"[6D:aggregate] OK {kpi_name}")
        except Exception as e:
            print(f"[6D:aggregate] FAILED {kpi_name}: {e}")
    return prepared


def _build_compare_data(
    kpi_1_spec: Optional[Dict[str, Any]],
    kpi_2_spec: Optional[Dict[str, Any]],
    df: pd.DataFrame,
    dimension: Optional[str],
) -> List[Dict[str, Any]]:
    """Build prepared_data for COMPARE: compute kpi_1 and kpi_2 independently."""
    specs = [s for s in [kpi_1_spec, kpi_2_spec] if s is not None]
    return _build_prepared_data(specs, df, dimension)


def _lookup_kpi_spec(
    kpi_name: Optional[str],
    dashboard_plan: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Find KPI spec by name (case-insensitive) from dashboard_plan."""
    if not kpi_name:
        return None
    for kpi in dashboard_plan.get("kpis", []):
        if (kpi.get("name") or "").lower() == kpi_name.lower():
            return kpi
    # Return first KPI as fallback if name not found
    kpis = dashboard_plan.get("kpis", [])
    if kpis:
        print(f"[6D] KPI '{kpi_name}' not in plan, using first available")
        return kpis[0]
    return None


def _build_insight_like_response(
    prepared_data: List[Dict[str, Any]],
    execution_trace: List[str],
    errors: List[str],
) -> Dict[str, Any]:
    """
    Build a minimal valid pipeline_result dict for PARTIAL_RUN.
    Mirrors the shape of run_pipeline() output.
    Insight/chart layers are intentionally lightweight for PARTIAL_RUN
    (full chart rendering requires LangGraph; partial runs return data only).
    """
    insights = []
    chart_specs = []

    for item in prepared_data:
        kpi = item.get("kpi", "")
        if item.get("type") == "scalar":
            val = item.get("value", 0)
            insights.append(
                {
                    "kpi": kpi,
                    "type": "scalar",
                    "details": {
                        "value": val,
                        "formatted": f"{val:,.2f}"
                        if isinstance(val, float)
                        else str(val),
                    },
                    "confidence": 1.0,
                    "score": 1.0,
                }
            )
            chart_specs.append({"kpi": kpi, "type": "metric", "value": val})

        elif item.get("type") == "timeseries":
            data = item.get("data", [])
            if len(data) >= 2:
                values = [r.get("value") for r in data if r.get("value") is not None]
                if values:
                    insights.append(
                        {
                            "kpi": kpi,
                            "type": "range",
                            "details": {
                                "min": min(values),
                                "max": max(values),
                                "points": len(data),
                            },
                            "confidence": min(1.0, len(data) / 5),
                            "score": 0.8,
                        }
                    )

    return {
        "query_results": [],  # Not used in partial path
        "prepared_data": prepared_data,
        "transformed_data": [],  # Skipped in partial path
        "insights": insights,
        "insight_summary": None,  # LLM optional layer — skipped
        "chart_specs": chart_specs,
        "execution_trace": execution_trace,
        "errors": errors,
    }


# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────


def adaptive_execute(
    plan: ExecutionPlan,
    resolved_intent: Dict[str, Any],
    dashboard_plan: Dict[str, Any],
    df: pd.DataFrame,  # Raw DataFrame from session
    prev_state: Optional[ExecutionState],
    session_id: str,
    run_id: str,
) -> AdaptiveResult:
    """
    Execute the pipeline according to the ExecutionPlan.

    Args:
        plan:             From ExecutionPlanner.plan()
        resolved_intent:  From ContextResolver (RESOLVED only)
        dashboard_plan:   From generate_dashboard_plan() as dict
        df:               Full raw DataFrame from upload session
        prev_state:       ExecutionState from previous turn (None on first)
        session_id:       For logging
        run_id:           Unique ID for this execution

    Returns:
        AdaptiveResult — always populated, never raises.
    """
    ops = plan.operations
    print(
        f"[6D:exec] session={session_id} mode={plan.mode} "
        f"reuse={plan.reuse} ops={ops} reason={plan.reason}"
    )

    # ── FULL_RUN: delegate entirely to LangGraph pipeline ────────────
    if plan.mode == MODE_FULL:
        return _execute_full(
            dashboard_plan=dashboard_plan,
            df=df,
            resolved_intent=resolved_intent,
            session_id=session_id,
            run_id=run_id,
            ops=ops,
        )

    # ── PARTIAL_RUN ──────────────────────────────────────────────────
    # Safety: if prev_state is somehow invalid, fall back to full run
    if prev_state is None or not prev_state.is_valid():
        print(f"[6D:exec] SAFETY FALLBACK: prev_state invalid → FULL_RUN")
        return _execute_full(
            dashboard_plan=dashboard_plan,
            df=df,
            resolved_intent=resolved_intent,
            session_id=session_id,
            run_id=run_id,
            ops=["load", STEP_FILTER, STEP_GROUPBY, STEP_AGGREGATE, STEP_RENDER],
        )

    return _execute_partial(
        plan=plan,
        resolved_intent=resolved_intent,
        dashboard_plan=dashboard_plan,
        prev_state=prev_state,
        ops=ops,
    )


def _execute_full(
    dashboard_plan: Dict[str, Any],
    df: pd.DataFrame,
    resolved_intent: Dict[str, Any],
    session_id: str,
    run_id: str,
    ops: List[str],
) -> AdaptiveResult:
    """Run LangGraph pipeline end-to-end. Returns AdaptiveResult."""
    from dataclasses import asdict

    # Register raw df for LangGraph nodes to consume
    register_df(run_id, df)

    initial_state: PipelineState = {
        "session_id": session_id,
        "dataset": {
            "filename": dashboard_plan.get("_meta", {}).get("filename", ""),
            "columns": list(df.columns),
            "shape": list(df.shape),
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "missing_pct": {},
        },
        "dashboard_plan": dashboard_plan,
        "shared_context": {"run_id": run_id, "applied_filters": []},
        "query_results": [],
        "prepared_data": None,
        "insights": [],
        "chart_specs": [],
        "insight_summary": None,
        "transformed_data": None,
        "retry_flags": {},
        "execution_trace": [],
        "is_refinement": False,
        "target_components": [],
        "retry_count": 0,
        "errors": [],
        "run_id": run_id,
        "parent_run_id": None,
        "intent": resolved_intent,
        "intent_raw": resolved_intent,
        "resolution_status": "RESOLVED",
    }

    try:
        result = run_pipeline(initial_state)
    finally:
        deregister_df(run_id)

    # Extract cacheable artifacts from full run
    # base_df = raw df (no filter applied)
    # filtered_df = we derive it post-hoc from intent
    base_df = df.copy()
    filtered_df = _apply_filter(df, resolved_intent)
    final_output = result.get("prepared_data") or []

    pipeline_result = dict(result)

    print(
        f"[6D:exec] FULL_RUN complete — "
        f"charts={len(result.get('chart_specs') or [])}, "
        f"insights={len(result.get('insights') or [])}"
    )

    return AdaptiveResult(
        base_df=base_df,
        filtered_df=filtered_df,
        final_output=final_output,
        pipeline_result=pipeline_result,
        mode_used=MODE_FULL,
        operations_run=ops,
        plan_reason="full_run",
    )


def _execute_partial(
    plan: ExecutionPlan,
    resolved_intent: Dict[str, Any],
    dashboard_plan: Dict[str, Any],
    prev_state: ExecutionState,
    ops: List[str],
) -> AdaptiveResult:
    """Execute only the steps in plan.operations using cached DataFrames."""
    errors: List[str] = []
    trace: List[str] = [f"6D:partial:{plan.reuse}"]

    intent_type = resolved_intent.get("intent", "")
    dimension = resolved_intent.get("dimension")
    kpi_name = resolved_intent.get("kpi")

    # ── Determine the starting DataFrame based on reuse level ────────
    if plan.reuse == REUSE_LAST_RESULT:
        # No recomputation needed — pass cached result to render layer
        trace.append(STEP_RENDER)
        last = prev_state.last_result or []
        print(f"[6D:partial] Cache hit — reusing last_result ({len(last)} items)")
        pipeline_result = _build_insight_like_response(
            prepared_data=last,
            execution_trace=trace,
            errors=[],
        )
        return AdaptiveResult(
            base_df=prev_state.base_df,
            filtered_df=prev_state.filtered_df,
            final_output=last,
            pipeline_result=pipeline_result,
            mode_used=MODE_PARTIAL,
            operations_run=trace,
            plan_reason=plan.reason,
        )

    if plan.reuse == REUSE_BASE_DF:
        working_df = prev_state.base_df.copy()
        # Apply new filter
        if STEP_FILTER in ops:
            working_df = _apply_filter(working_df, resolved_intent)
            trace.append(STEP_FILTER)
        new_base_df = prev_state.base_df.copy()
        new_filtered_df = working_df.copy()

    elif plan.reuse == REUSE_FILTERED_DF:
        working_df = prev_state.filtered_df.copy()
        new_base_df = prev_state.base_df.copy()
        new_filtered_df = working_df.copy()

    else:
        # Unknown reuse level — safety fallback (should never happen)
        errors.append(f"Unknown reuse level: {plan.reuse}")
        pipeline_result = _build_insight_like_response([], trace, errors)
        return AdaptiveResult(
            base_df=prev_state.base_df,
            filtered_df=prev_state.filtered_df,
            final_output=[],
            pipeline_result=pipeline_result,
            mode_used=MODE_PARTIAL,
            operations_run=trace,
            plan_reason=plan.reason,
        )

    # ── COMPARE path ──────────────────────────────────────────────────
    if intent_type == "COMPARE" and (
        STEP_COMPUTE_KPI_1 in ops or STEP_COMPUTE_KPI_2 in ops
    ):
        kpi_1_name = resolved_intent.get("kpi_1")
        kpi_2_name = resolved_intent.get("kpi_2")
        kpi_1_spec = _lookup_kpi_spec(kpi_1_name, dashboard_plan)
        kpi_2_spec = _lookup_kpi_spec(kpi_2_name, dashboard_plan)

        trace.append(STEP_COMPUTE_KPI_1)
        trace.append(STEP_COMPUTE_KPI_2)

        prepared_data = _build_compare_data(
            kpi_1_spec, kpi_2_spec, working_df, dimension
        )
        trace.append(STEP_RENDER)

        pipeline_result = _build_insight_like_response(prepared_data, trace, errors)
        return AdaptiveResult(
            base_df=new_base_df,
            filtered_df=new_filtered_df,
            final_output=prepared_data,
            pipeline_result=pipeline_result,
            mode_used=MODE_PARTIAL,
            operations_run=trace,
            plan_reason=plan.reason,
        )

    # ── Standard partial path (groupby + aggregate) ───────────────────
    kpi_specs_to_run: List[Dict[str, Any]] = []

    if kpi_name:
        spec = _lookup_kpi_spec(kpi_name, dashboard_plan)
        if spec:
            kpi_specs_to_run = [spec]
    else:
        # No specific KPI requested — recompute all planned KPIs
        kpi_specs_to_run = dashboard_plan.get("kpis", [])

    if STEP_GROUPBY in ops:
        trace.append(STEP_GROUPBY)
    if STEP_AGGREGATE in ops:
        trace.append(STEP_AGGREGATE)

    prepared_data = _build_prepared_data(kpi_specs_to_run, working_df, dimension)
    trace.append(STEP_RENDER)

    pipeline_result = _build_insight_like_response(prepared_data, trace, errors)

    print(f"[6D:partial] Complete — ops={trace}, prepared={len(prepared_data)}")

    return AdaptiveResult(
        base_df=new_base_df,
        filtered_df=new_filtered_df,
        final_output=prepared_data,
        pipeline_result=pipeline_result,
        mode_used=MODE_PARTIAL,
        operations_run=trace,
        plan_reason=plan.reason,
    )
