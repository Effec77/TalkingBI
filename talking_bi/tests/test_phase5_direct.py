"""
Phase 5 Direct Pipeline Test
Tests chart rendering, insight ranking, and UI response without HTTP server.
"""

import sys
import os
import pandas as pd
from io import BytesIO

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.nodes import query_node, prep_node, insight_node, chart_node
from graph.state import PipelineState
from graph.df_registry import register_df, deregister_df
from services.chart_renderer import ChartRenderer


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ── Setup test data ───────────────────────────────────────────
sep("SETUP TEST DATA")

# Create test DataFrame
df = pd.DataFrame(
    {
        "date": pd.date_range("2024-01-01", periods=12, freq="ME"),
        "sales": [
            1000,
            1200,
            900,
            1500,
            1800,
            2000,
            1900,
            2200,
            2500,
            2300,
            2800,
            3000,
        ],
        "region": ["North"] * 6 + ["South"] * 6,
        "product": ["A", "B", "A", "B", "A", "B"] * 2,
        "quantity": [10, 12, 9, 15, 18, 20, 19, 22, 25, 23, 28, 30],
    }
)

print(f"DataFrame shape: {df.shape}")
print(f"Columns: {list(df.columns)}")

# ── Setup initial state ──────────────────────────────────────
sep("SETUP INITIAL STATE")

run_id = "test-phase5-001"
register_df(run_id, df)

initial_state: PipelineState = {
    "session_id": "test-session-001",
    "dataset": {
        "filename": "test.csv",
        "columns": list(df.columns),
        "shape": list(df.shape),
    },
    "dashboard_plan": {
        "name": "Sales Dashboard",
        "kpis": [
            {
                "name": "Total Sales",
                "source_column": "sales",
                "aggregation": "sum",
                "time_column": "date",
            },
            {
                "name": "Avg Quantity",
                "source_column": "quantity",
                "aggregation": "mean",
            },
            {
                "name": "Sales by Region",
                "source_column": "sales",
                "aggregation": "sum",
                "segment_by": "region",
            },
        ],
    },
    "shared_context": {},
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
}

print(f"Run ID: {run_id}")

# ── Run Pipeline ───────────────────────────────────────────────
sep("STEP 1: QUERY NODE")
state_after_query = query_node(initial_state)
print(f"Query results: {len(state_after_query['query_results'])}")
for r in state_after_query["query_results"]:
    print(f"  - {r['kpi']}: {r['status']}")

sep("STEP 2: PREP NODE")
state_after_prep = prep_node({**initial_state, **state_after_query})
print(f"Prepared data: {len(state_after_prep['prepared_data'])}")
print(f"Transformed data: {len(state_after_prep['transformed_data'])}")
for t in state_after_prep["transformed_data"]:
    print(
        f"  - {t['kpi']}: min={t['min']}, max={t['max']}, points={t['points']}, missing={t.get('missing', 0)}"
    )

sep("STEP 3: INSIGHT NODE")
state_after_insight = insight_node(
    {**initial_state, **state_after_query, **state_after_prep}
)
print(f"Insights generated: {len(state_after_insight['insights'])}")

# Check insight scores
insights_with_scores = [i for i in state_after_insight["insights"] if "score" in i]
print(f"Insights with scores: {len(insights_with_scores)}")

for i in state_after_insight["insights"][:5]:
    score_info = f" (score={i.get('score', 'N/A')})" if "score" in i else ""
    print(
        f"  - [{i['type']:12}] {i['kpi'][:20]:20} conf={i['confidence']:.2f}{score_info}"
    )

sep("STEP 4: CHART NODE")
state_after_chart = chart_node(
    {**initial_state, **state_after_query, **state_after_prep, **state_after_insight}
)
print(f"Chart specs: {len(state_after_chart['chart_specs'])}")

# Check chart images
charts_with_images = 0
for c in state_after_chart["chart_specs"]:
    if c.get("type") == "line":
        has_image = bool(c.get("image"))
        img_info = f"image={len(c['image'])} chars" if has_image else "image=None"
        print(f"  - {c['kpi']}: {c['type']} [{img_info}]")
        if has_image:
            charts_with_images += 1
    else:
        print(f"  - {c['kpi']}: {c['type']} value={c.get('value')}")

print(f"\nCharts with base64 images: {charts_with_images}")

# ── Verify KPI Ranking ───────────────────────────────────────
sep("VERIFICATION: KPI RANKING")

# Initialize tracking variables
kpi_ranking_pass = True  # Default to pass if not enough data
transformed = state_after_prep["transformed_data"]

if len(transformed) >= 2:

    def calc_importance(item):
        min_val = item.get("min", 0) or 0
        max_val = item.get("max", 0) or 0
        points = item.get("points", 1)
        return abs(max_val - min_val) * points

    importances = [calc_importance(t) for t in transformed]
    is_sorted = all(
        importances[i] >= importances[i + 1] for i in range(len(importances) - 1)
    )
    kpi_ranking_pass = is_sorted
    print(f"KPIs ranked by importance: {'PASS' if is_sorted else 'FAIL'}")
    for i, t in enumerate(transformed):
        print(f"  {i + 1}. {t['kpi']}: importance={importances[i]:.0f}")
else:
    print("Not enough KPIs to check ranking")

# ── Verify Insight Scoring ───────────────────────────────────
sep("VERIFICATION: INSIGHT SCORING")
insights = state_after_insight["insights"]
insights_with_scores = [i for i in insights if "score" in i]
print(f"Insights with score field: {len(insights_with_scores)}/{len(insights)}")

if insights_with_scores:
    # Check if sorted by score
    scores = [i["score"] for i in insights_with_scores]
    is_sorted = all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1))
    print(f"Insights sorted by score: {'PASS' if is_sorted else 'FAIL'}")
    for i, insight in enumerate(insights_with_scores[:5]):
        print(
            f"  {i + 1}. [{insight['type']:12}] {insight['kpi'][:20]:20} score={insight['score']:.2f}"
        )

# ── Verify Chart Renderer ────────────────────────────────────
sep("VERIFICATION: CHART RENDERER")
renderer = ChartRenderer()
print(f"ChartRenderer initialized: PASS")

# Test render_line
if state_after_prep["prepared_data"]:
    timeseries_item = [
        p for p in state_after_prep["prepared_data"] if p["type"] == "timeseries"
    ]
    if timeseries_item:
        data = timeseries_item[0]["data"]
        keys = list(data[0].keys())
        x_key = [k for k in keys if k != "value"][0]
        y_key = "value"
        image = renderer.render_line(data, x_key, y_key)
        if image:
            print(f"render_line() output: PASS ({len(image)} chars)")
        else:
            print(f"render_line() output: FAIL (returned None)")
    else:
        print("No timeseries data to test render_line()")
else:
    print("No prepared data to test renderer")

# Test render_metric
metric_value = 42.5
result = renderer.render_metric(metric_value)
if result == metric_value:
    print(f"render_metric() output: PASS")
else:
    print(f"render_metric() output: FAIL")

# ── Cleanup ───────────────────────────────────────────────────
deregister_df(run_id)

sep("DONE - Phase 5 Pipeline Test Complete")
print("\nSummary:")
print(f"  - Chart rendering: {'PASS' if charts_with_images > 0 else 'FAIL'}")
print(
    f"  - Insight scoring: {'PASS' if len(insights_with_scores) == len(insights) else 'FAIL'}"
)
print(f"  - KPI ranking: {'PASS' if kpi_ranking_pass else 'FAIL'}")
