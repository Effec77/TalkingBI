"""
Test explicit count/nunique blocking (with higher importance)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.nodes import chart_node
from graph.state import PipelineState

# Test state with count/nunique KPIs that have HIGH importance (to bypass importance filter)
test_state: PipelineState = {
    "session_id": "test-blocking-2",
    "dataset": {"filename": "test.csv", "columns": ["a", "b"], "shape": [100, 2]},
    "dashboard_plan": {
        "name": "Test",
        "kpis": [
            {"name": "Revenue", "source_column": "sales", "aggregation": "sum"},
            {"name": "Total Records", "source_column": None, "aggregation": "count"},
        ],
    },
    "shared_context": {},
    "query_results": [],
    "prepared_data": [
        {
            "kpi": "Revenue",
            "type": "timeseries",
            "data": [{"month": "Jan", "value": 100}, {"month": "Feb", "value": 1000}],
        },
        {
            "kpi": "Total Records",
            "type": "timeseries",
            "data": [{"month": "Jan", "value": 50}, {"month": "Feb", "value": 150}],
        },
    ],
    "transformed_data": [
        {"kpi": "Revenue", "min": 100, "max": 1000, "points": 100},  # High importance
        {
            "kpi": "Total Records",
            "min": 50,
            "max": 150,
            "points": 100,
        },  # count with HIGH importance
    ],
    "insights": [],
    "chart_specs": [],
    "insight_summary": None,
    "retry_flags": {},
    "execution_trace": [],
    "is_refinement": False,
    "target_components": [],
    "retry_count": 0,
    "errors": [],
    "run_id": "test-blocking-002",
    "parent_run_id": None,
}

print("Testing explicit count/nunique blocking (with high importance)...")
print(f"Dashboard plan:")
for k in test_state["dashboard_plan"]["kpis"]:
    print(f"  - {k['name']}: {k['aggregation']}")

print(f"\nTransformed data importance:")
for t in test_state["transformed_data"]:
    importance = abs(t["max"] - t["min"]) * (t["points"] ** 0.5)
    print(f"  - {t['kpi']}: importance={importance:.1f}")

result = chart_node(test_state)

print(f"\nCharts generated: {len(result['chart_specs'])}")
for chart in result["chart_specs"]:
    print(f"  - {chart['kpi']}: {chart['type']}")

# Verify explicit blocking worked
has_revenue = any(c["kpi"] == "Revenue" for c in result["chart_specs"])
has_count = any(c["kpi"] == "Total Records" for c in result["chart_specs"])

if has_revenue and not has_count:
    print("\n[OK] SUCCESS: Count KPI blocked by explicit aggregation check!")
    sys.exit(0)
else:
    print(f"\n[FAIL] Revenue: {has_revenue}, Count blocked: {not has_count}")
    sys.exit(1)
