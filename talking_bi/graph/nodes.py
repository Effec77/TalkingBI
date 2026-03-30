"""
LangGraph Nodes — Phase 5: Visualization + Output Intelligence Layer

Changes from Phase 4:
  - insight_node: Added insight scoring and ranking (score field, sorted by importance)
  - chart_node: Actual chart rendering with ChartRenderer (base64 PNG images)
  - prep_node: Added KPI ranking by importance (variance-based)
  - query_node: Unchanged from Phase 3 (adaptive retry)

Nodes return dicts of ONLY the keys they mutate (LangGraph merges).
DataFrames are retrieved from the df_registry by run_id.
"""

import math
import pandas as pd

from .df_registry import get_df
from .state import PipelineState
from services.chart_renderer import ChartRenderer


# ─────────────────────────────────────────────────────────────
# NODE 1 — Query Node (Adaptive)
# Executes each KPI with retry logic.
# On first failure: retries once by stripping groupby.
# UNCHANGED from Phase 3.
# ─────────────────────────────────────────────────────────────


def query_node(state: PipelineState) -> dict:
    print(f"[NODE:query] run_id={state['run_id']}")

    run_id = state["run_id"]
    kpis = state["dashboard_plan"]["kpis"]
    errors = list(state.get("errors", []))
    retry_flags = dict(state.get("retry_flags", {}))
    trace = list(state.get("execution_trace", []))
    trace.append("query")

    try:
        df = get_df(run_id)
    except KeyError as e:
        errors.append(str(e))
        return {
            "query_results": [],
            "errors": errors,
            "retry_flags": retry_flags,
            "execution_trace": trace,
        }

    results = []

    for kpi in kpis:
        kpi_name = kpi.get("name", "unknown")
        col = kpi.get("source_column")
        agg = kpi.get("aggregation", "").lower()
        time_col = kpi.get("time_column")
        segment_by = kpi.get("segment_by")

        try:
            result = _execute_kpi(df, col, agg, time_col, segment_by)

            results.append(
                {
                    "kpi": kpi_name,
                    "source_column": col,
                    "aggregation": agg,
                    "data": result,
                    "status": "success",
                }
            )
            print(f"[NODE:query] OK {kpi_name} ({agg})")

        except Exception as e:
            # ── Retry logic: max 1 retry per KPI ──────────────────
            if not retry_flags.get(kpi_name, False):
                print(f"[RETRY] Retrying KPI: {kpi_name} (stripping groupby)")
                retry_flags[kpi_name] = True

                try:
                    result = _execute_kpi(df, col, agg, time_col=None, segment_by=None)
                    results.append(
                        {
                            "kpi": kpi_name,
                            "source_column": col,
                            "aggregation": agg,
                            "data": result,
                            "status": "retry_success",
                        }
                    )
                    print(f"[RETRY] OK {kpi_name} (retry succeeded)")
                    continue
                except Exception as retry_e:
                    print(f"[RETRY] FAILED {kpi_name}: {retry_e}")

            err_msg = f"KPI '{kpi_name}' failed: {e}"
            print(f"[NODE:query] FAILED {err_msg}")
            results.append(
                {
                    "kpi": kpi_name,
                    "error": err_msg,
                    "status": "failed",
                }
            )
            errors.append(err_msg)

    if not results:
        errors.append("No KPI results generated - check dashboard_plan.kpis")

    # FIX 4 — Ensure KPI alignment with plan
    planned_kpis = {
        k.get("name", "") for k in state.get("dashboard_plan", {}).get("kpis", [])
    }
    executed_kpis = {r["kpi"] for r in results}

    # Check for any unexpected KPIs (should be subset of planned)
    unexpected = executed_kpis - planned_kpis
    if unexpected:
        errors.append(f"Unexpected KPIs in results: {unexpected}")
        print(f"[NODE:query] WARNING: Unexpected KPIs detected: {unexpected}")

    return {
        "query_results": results,
        "errors": errors,
        "retry_flags": retry_flags,
        "execution_trace": trace,
    }


def _execute_kpi(df, col, agg, time_col, segment_by):
    """
    Pure pandas KPI execution helper.
    No eval(), no exec(), no LLM.
    """
    if col and col not in df.columns:
        raise ValueError(f"Column '{col}' not found in dataset")
    if time_col and time_col not in df.columns:
        time_col = None
    if segment_by and segment_by not in df.columns:
        segment_by = None

    group_by = time_col or segment_by

    if agg == "count":
        if group_by:
            return df.groupby(group_by).size().reset_index(name="value")
        return len(df)

    elif agg == "sum":
        if group_by:
            return df.groupby(group_by)[col].sum().reset_index(name="value")
        return float(df[col].sum())

    elif agg in ("avg", "mean"):
        if group_by:
            return df.groupby(group_by)[col].mean().reset_index(name="value")
        return float(df[col].mean())

    elif agg == "min":
        return float(df[col].min())

    elif agg == "max":
        return float(df[col].max())

    elif agg == "nunique":
        if group_by:
            return df.groupby(group_by)[col].nunique().reset_index(name="value")
        return int(df[col].nunique())

    else:
        print(f"[NODE:query] Unknown agg '{agg}' - defaulting to count")
        return len(df)


# ─────────────────────────────────────────────────────────────
# NODE 2 — Prep Node (DeepPrep v2)
# PHASE 4 FIX: NaN/None values are TRACKED, not replaced with 0.
# transformed_data now includes 'missing' count for quality scoring.
# ─────────────────────────────────────────────────────────────


def prep_node(state: PipelineState) -> dict:
    print(f"[NODE:prep] run_id={state['run_id']}")

    trace = list(state.get("execution_trace", []))
    trace.append("prep")
    errors = list(state.get("errors", []))

    query_results = state.get("query_results", [])

    # ── Conditional guard: if ALL failed, abort early ─────────
    if query_results and all(r["status"] == "failed" for r in query_results):
        err = "All KPI executions failed - aborting prep"
        print(f"[NODE:prep] ABORT: {err}")
        errors.append(err)
        return {
            "prepared_data": [],
            "transformed_data": [],
            "errors": errors,
            "execution_trace": trace,
        }

    prepared = []
    transformed = []

    for result in query_results:
        if result["status"] not in ("success", "retry_success"):
            print(f"[NODE:prep] Skipping failed KPI: {result['kpi']}")
            continue

        data = result["data"]
        kpi_name = result["kpi"]

        try:
            # ── SCALAR ────────────────────────────────────────
            if isinstance(data, (int, float)):
                prepared.append(
                    {
                        "kpi": kpi_name,
                        "type": "scalar",
                        "value": data,
                    }
                )
                print(f"[NODE:prep] OK {kpi_name} -> scalar ({data})")

            # ── TIMESERIES (DataFrame) ────────────────────────
            elif isinstance(data, pd.DataFrame):
                records = data.to_dict(orient="records")

                # Phase 4 FIX: track missing, don't zero-fill
                missing_count = 0
                valid_values = []
                clean_records = []

                for r in records:
                    val = r.get("value")

                    # Track missing/invalid values
                    if val is None or (
                        isinstance(val, float) and (math.isnan(val) or math.isinf(val))
                    ):
                        missing_count += 1
                        clean_records.append({**r, "value": val})
                    else:
                        valid_values.append(val)
                        clean_records.append({**r, "value": val})

                # DeepPrep stats — now with missing count
                transformed.append(
                    {
                        "kpi": kpi_name,
                        "min": min(valid_values) if valid_values else None,
                        "max": max(valid_values) if valid_values else None,
                        "points": len(records),
                        "missing": missing_count,
                    }
                )

                prepared.append(
                    {
                        "kpi": kpi_name,
                        "type": "timeseries",
                        "data": clean_records,
                    }
                )
                print(
                    f"[NODE:prep] OK {kpi_name} -> timeseries "
                    f"({len(clean_records)} rows, {missing_count} missing)"
                )

            else:
                prepared.append(
                    {
                        "kpi": kpi_name,
                        "type": "unknown",
                        "raw": str(data),
                    }
                )
                print(
                    f"[NODE:prep] WARN {kpi_name} -> unknown type {type(data).__name__}"
                )

        except Exception as e:
            err_msg = f"DeepPrep error for {kpi_name}: {e}"
            print(f"[NODE:prep] FAILED {err_msg}")
            errors.append(err_msg)

    # FIX 7 — Filter weak KPIs (zero variance with multiple points)
    original_transformed = state.get("transformed_data") or []
    transformed = [
        k for k in transformed if not (k["points"] > 1 and k["min"] == k["max"])
    ]
    if len(transformed) < len(original_transformed):
        filtered_count = len(original_transformed) - len(transformed)
        print(f"[NODE:prep] Filtered {filtered_count} weak KPIs (zero variance)")

    # FIX 6 — Improve KPI Ranking (relative importance)
    def kpi_importance(item):
        """Score KPI by relative variance × data points (higher = more important)."""
        min_val = item.get("min", 0) or 0
        max_val = item.get("max", 0) or 0
        points = item.get("points", 1)

        # Avoid division by zero
        mean_val = (max_val + min_val) / 2 if (max_val and min_val) else 1
        if mean_val == 0:
            mean_val = 1

        range_val = abs(max_val - min_val)
        # Relative importance: (range/mean) * sqrt(points)
        return (range_val / mean_val) * (points**0.5)

    transformed = sorted(transformed, key=lambda x: kpi_importance(x), reverse=True)
    print(f"[NODE:prep] Ranked {len(transformed)} KPIs by importance")

    return {
        "prepared_data": prepared,
        "transformed_data": transformed,
        "errors": errors,
        "execution_trace": trace,
    }


# ─────────────────────────────────────────────────────────────
# NODE 3 — Insight Node (Phase 4 Patch: Correctness + Quality)
# Insight types:
#   - scalar:          single aggregated value
#   - range:           min/max spread (requires >= 2 points)
#   - trend:           increasing / decreasing / stable / insufficient_data
#   - data_quality:    missing data alerts (always confidence 1.0)
# Confidence is data-volume weighted, not hardcoded.
# Flat data (min==max) skips trend but still gets range insight.
# Then calls InsightNarrator for LLM business summary (optional).
# ─────────────────────────────────────────────────────────────


def insight_node(state: PipelineState) -> dict:
    print(f"[NODE:insight] run_id={state['run_id']}")

    trace = list(state.get("execution_trace", []))
    trace.append("insight")

    insights = []
    prepared = state.get("prepared_data") or []
    transformed = state.get("transformed_data") or []

    # Build a lookup from transformed_data for quick access
    transform_lookup = {t["kpi"]: t for t in transformed}

    # Build a lookup from prepared_data for trend detection
    prepared_lookup = {p["kpi"]: p for p in prepared}

    # FIX 6 — Build aggregation map to skip flat count KPIs
    aggregation_map = {
        k.get("name", ""): k.get("aggregation", "").lower()
        for k in state.get("dashboard_plan", {}).get("kpis", [])
    }

    for item in prepared:
        kpi = item["kpi"]

        # FIX 1 — Validate KPI name is string and in plan
        assert isinstance(kpi, str), f"KPI name must be string, got {type(kpi)}"
        planned_kpis = {
            k.get("name", "") for k in state.get("dashboard_plan", {}).get("kpis", [])
        }
        if kpi not in planned_kpis:
            print(f"[NODE:insight] SKIP {kpi} -> not in planned KPIs (PHANTOM)")
            continue

        # FIX 6 — Skip flat count KPIs (single point or low value)
        if aggregation_map.get(kpi) == "count":
            stats = transform_lookup.get(kpi)
            if stats and stats.get("points", 0) <= 1:
                print(f"[NODE:insight] SKIP {kpi} -> flat count KPI (single point)")
                continue

        try:
            # ── SCALAR INSIGHTS ───────────────────────────────
            if item["type"] == "scalar":
                val = item["value"]
                if isinstance(val, float) and not val.is_integer():
                    formatted = f"{val:,.2f}"
                else:
                    formatted = f"{int(val):,}"
                insights.append(
                    {
                        "kpi": kpi,
                        "type": "scalar",
                        "details": {
                            "value": val,
                            "formatted": formatted,
                        },
                        "confidence": 1.0,
                    }
                )

            # ── TIMESERIES INSIGHTS ───────────────────────────
            elif item["type"] == "timeseries":
                stats = transform_lookup.get(kpi)
                if not stats:
                    continue

                points = stats["points"]
                missing = stats.get("missing", 0)
                min_val = stats["min"]
                max_val = stats["max"]

                # FIX 2 — Confidence is data-volume weighted, not hardcoded
                base_conf = 1.0 - (missing / points if points else 0)
                data_factor = min(1.0, points / 5)  # penalise low data
                confidence = round(base_conf * data_factor, 2)

                # FIX 4 — Check for flat data (no variation)
                skip_trend = (
                    min_val is not None and max_val is not None and min_val == max_val
                )

                # FIX 5 — Range insight: only if >= 2 points
                if points >= 2 and min_val is not None and max_val is not None:
                    insights.append(
                        {
                            "kpi": kpi,
                            "type": "range",
                            "details": {
                                "min": min_val,
                                "max": max_val,
                                "points": points,
                            },
                            "confidence": confidence,
                        }
                    )
                elif points == 1 and max_val is not None:
                    # Single point — emit as scalar instead of range
                    insights.append(
                        {
                            "kpi": kpi,
                            "type": "scalar",
                            "details": {"value": max_val},
                            "confidence": confidence,
                        }
                    )

                # FIX 1 + FIX 3 — Correct trend logic
                if not skip_trend:
                    data_records = item.get("data", [])
                    numeric_values = [
                        r["value"]
                        for r in data_records
                        if r.get("value") is not None
                        and not (
                            isinstance(r["value"], float) and math.isnan(r["value"])
                        )
                    ]

                    # FIX 1 — Guard: need >= 3 points for a real trend
                    if len(numeric_values) < 3:
                        direction = "insufficient_data"
                    else:
                        start_val = numeric_values[0]
                        end_val = numeric_values[-1]
                        if end_val > start_val:
                            direction = "increasing"
                        elif end_val < start_val:
                            direction = "decreasing"
                        else:
                            direction = "stable"  # FIX 3

                    trend_conf = (
                        round(confidence * 0.8, 2)
                        if direction not in ("insufficient_data", "stable")
                        else round(confidence * 0.5, 2)
                    )

                    insights.append(
                        {
                            "kpi": kpi,
                            "type": "trend",
                            "details": {
                                "direction": direction,
                                "start": numeric_values[0] if numeric_values else None,
                                "end": numeric_values[-1] if numeric_values else None,
                            },
                            "confidence": trend_conf,
                        }
                    )

                    # FIX 3 — Anomaly detection (spike detection)
                    if len(numeric_values) >= 5:
                        avg = sum(numeric_values) / len(numeric_values)
                        max_val_data = max(numeric_values)

                        if max_val_data > avg * 1.5:
                            insights.append(
                                {
                                    "kpi": kpi,
                                    "type": "anomaly",
                                    "details": {
                                        "spike": max_val_data,
                                        "average": round(avg, 2),
                                        "spike_ratio": round(max_val_data / avg, 2),
                                    },
                                    "confidence": min(1.0, confidence + 0.1),
                                }
                            )
                            print(
                                f"[NODE:insight] {kpi}: anomaly detected (spike={max_val_data:.0f}, avg={avg:.0f})"
                            )

                    # FIX 4 — Comparison insight (min vs max spread)
                    if len(numeric_values) >= 2:
                        max_v = max(numeric_values)
                        min_v = min(numeric_values)

                        insights.append(
                            {
                                "kpi": kpi,
                                "type": "comparison",
                                "details": {
                                    "max": max_v,
                                    "min": min_v,
                                    "spread": max_v - min_v,
                                    "spread_pct": round(
                                        ((max_v - min_v) / abs(min_v) * 100)
                                        if min_v != 0
                                        else 0,
                                        1,
                                    ),
                                },
                                "confidence": confidence,
                            }
                        )

                    # FIX 6 — Data quality insight (if any missing)
                    insights.append(
                        {
                            "kpi": kpi,
                            "type": "data_quality",
                            "details": {
                                "missing_points": missing,
                                "total_points": points,
                                "completeness_pct": round(
                                    (1 - missing / points) * 100, 1
                                ),
                            },
                            "confidence": 1.0,  # 100% certain about missing data
                        }
                    )

            elif item["type"] == "unknown":
                insights.append(
                    {
                        "kpi": kpi,
                        "type": "data_quality",
                        "details": {"error": "Could not interpret data type"},
                        "confidence": 0.0,
                    }
                )

            print(f"[NODE:insight] OK {kpi}")

        except Exception as e:
            print(f"[NODE:insight] FAILED insight for {kpi}: {e}")

    # ── FIX 8 — Validation Layer ──────────────────────────────
    assert all(i["confidence"] >= 0 for i in insights), (
        "Insight has negative confidence"
    )
    assert all("type" in i for i in insights), "Insight missing 'type' field"
    assert all("details" in i for i in insights), "Insight missing 'details' field"

    # ── PHASE 5 — Insight Scoring & Ranking ──────────────────────
    def score_insight(insight):
        """Score insight by confidence + type bonuses."""
        base = insight["confidence"]
        kpi = insight["kpi"]

        # FIX 5 — Updated scoring with stronger anomaly bonus
        if insight["type"] == "anomaly":
            base += 0.25  # Increased from 0.2
        elif insight["type"] == "trend":
            base += 0.15
        elif insight["type"] == "comparison":
            base += 0.1
        elif insight["type"] == "data_quality":
            base += 0.05

        # FIX 3 — Penalize count KPIs (they should never dominate)
        agg = aggregation_map.get(kpi, "").lower()
        if agg in ["count", "nunique"]:
            base -= 0.3

        return round(min(max(base, 0.0), 1.0), 2)

    for i in insights:
        i["score"] = score_insight(i)

    # Sort by score descending (highest importance first)
    insights.sort(key=lambda x: x["score"], reverse=True)
    print(f"[NODE:insight] Ranked {len(insights)} insights by score")

    # ── FIX 6 — Limit low-value insights ─
    insights = [
        i for i in insights if not (i["type"] == "range" and i["confidence"] < 0.5)
    ]
    print(f"[NODE:insight] Filtered low-confidence range insights")

    # ── FIX 3 — Reduce insight redundancy (keep highest priority per KPI) ─
    priority_order = [
        "anomaly",
        "trend",
        "comparison",
        "range",
        "scalar",
        "data_quality",
    ]
    best_per_kpi = {}

    for i in insights:
        kpi = i["kpi"]
        if kpi not in best_per_kpi:
            best_per_kpi[kpi] = i
        else:
            existing = best_per_kpi[kpi]
            # Keep the one with higher priority (lower index in priority_order)
            try:
                new_priority = priority_order.index(i["type"])
            except ValueError:
                new_priority = len(priority_order)  # Unknown types go to end
            try:
                existing_priority = priority_order.index(existing["type"])
            except ValueError:
                existing_priority = len(priority_order)

            if new_priority < existing_priority:
                best_per_kpi[kpi] = i

    insights = list(best_per_kpi.values())
    print(
        f"[NODE:insight] Reduced to {len(insights)} unique KPI insights (priority kept)"
    )

    # ── FIX 4 — Limit total insights ─
    MAX_INSIGHTS = 5
    insights = insights[:MAX_INSIGHTS]
    print(f"[NODE:insight] Limited to top {MAX_INSIGHTS} insights")

    # ── FIX 7 — LLM Input Cleanup: filter weak/noisy insights ─
    valid_insights = [
        i for i in insights if not (i["type"] == "scalar" and i["confidence"] <= 0.3)
    ]

    # ── LLM Narrative Layer ────────────────────────────────────
    # LLM is OPTIONAL. If it fails, summary is None — pipeline continues.
    insight_summary = None
    try:
        from services.llm_manager import LLMManager
        from services.insight_narrator import InsightNarrator

        narrator = InsightNarrator(LLMManager())
        insight_summary = narrator.generate(valid_insights)  # FIX 7: filtered
        if insight_summary:
            print(
                f"[NODE:insight] LLM narrative generated ({len(insight_summary)} chars)"
            )
        else:
            print("[NODE:insight] LLM narrative skipped (no response)")
    except Exception as e:
        print(f"[NODE:insight] LLM narrative failed (non-blocking): {e}")
        # Non-blocking — insights remain valid without narrative

    return {
        "insights": insights,
        "insight_summary": insight_summary,
        "execution_trace": trace,
    }


# ─────────────────────────────────────────────────────────────
# NODE 4 — Chart Node (Phase 5 Upgrade)
# Renders actual chart images from prepared_data.
# Now generates base64-encoded PNG images, not just metadata.
# ─────────────────────────────────────────────────────────────

renderer = ChartRenderer()


def chart_node(state: PipelineState) -> dict:
    print(f"[NODE:chart] run_id={state['run_id']}")

    charts = []
    prepared = state.get("prepared_data") or []

    # FIX 1 — Build importance map from transformed_data
    importance_map = {
        k["kpi"]: (abs(k.get("max", 0) - k.get("min", 0)) * (k.get("points", 1) ** 0.5))
        for k in state.get("transformed_data", [])
    }

    # FIX 1 — Enforce minimum importance threshold
    MIN_IMPORTANCE = 10

    # FIX 3 — Build aggregation map to skip count KPIs
    aggregation_map = {
        k.get("name", ""): k.get("aggregation", "").lower()
        for k in state.get("dashboard_plan", {}).get("kpis", [])
    }

    for item in prepared:
        kpi = item["kpi"]

        # FIX 2 — Validate KPI name is string
        assert isinstance(kpi, str), f"KPI name must be string, got {type(kpi)}"

        # FIX 1 — Skip low-importance charts
        importance = importance_map.get(kpi, 0)
        if importance < MIN_IMPORTANCE:
            print(
                f"[NODE:chart] SKIP {kpi} -> low importance ({importance:.1f} < {MIN_IMPORTANCE})"
            )
            continue

        # FIX 3 — Hard block count KPI visualization (NO EXCEPTIONS)
        agg = aggregation_map.get(kpi, "").lower()
        if agg in ["count", "nunique"]:
            print(f"[NODE:chart] SKIP {kpi} -> count/nunique aggregation (blocked)")
            continue

        try:
            if item["type"] == "timeseries":
                data = item["data"]

                if not data or len(data) < 2:
                    print(f"[NODE:chart] SKIP {kpi} -> insufficient data")
                    continue

                # FIX 5 — Skip meaningless visuals (zero variance)
                values = [d["value"] for d in data if d.get("value") is not None]
                if len(values) < 2 or min(values) == max(values):
                    print(f"[NODE:chart] SKIP {kpi} -> zero variance (min=max)")
                    continue

                # Find x_key (non-value column) and y_key
                keys = list(data[0].keys())
                x_key = (
                    [k for k in keys if k != "value"][0] if len(keys) > 1 else keys[0]
                )
                y_key = "value"

                # FIX 1 — Detect if categorical vs time-series
                x_values = [d[x_key] for d in data]
                is_categorical = len(set(x_values)) <= 10
                chart_type = "bar" if is_categorical else "line"

                # FIX 3 — Use correct renderer
                if chart_type == "line":
                    image = renderer.render_line(data, x_key, y_key, kpi)
                else:
                    image = renderer.render_bar(data, x_key, y_key, kpi)

                # FIX D — Skip charts with missing images
                if image is None:
                    print(f"[NODE:chart] SKIP {kpi} -> image rendering failed")
                    continue

                charts.append(
                    {
                        "kpi": kpi,
                        "type": chart_type,
                        "image": image,
                        "x_key": x_key,
                        "y_key": y_key,
                    }
                )
                print(
                    f"[NODE:chart] OK {kpi} -> {chart_type} chart (image: {bool(image)})"
                )

            elif item["type"] == "scalar":
                charts.append(
                    {
                        "kpi": kpi,
                        "type": "metric",
                        "value": item["value"],
                    }
                )
                print(f"[NODE:chart] OK {kpi} -> metric card")

        except Exception as e:
            print(f"[NODE:chart] FAILED chart for {kpi}: {e}")

    # FIX 8 — Ensure chart consistency (filter out charts without images)
    charts = [c for c in charts if c.get("image") is not None or c["type"] == "metric"]

    # Safety: ensure chart_specs is always a list
    if not charts:
        charts = []

    assert isinstance(charts, list), "chart_specs must be a list"

    return {
        "chart_specs": charts,
        "execution_trace": list(state.get("execution_trace", [])) + ["chart"],
    }
