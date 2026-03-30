"""
Quick test to verify count/nunique KPI blocking
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.nodes import chart_node
from graph.state import PipelineState

# Test state with count/nunique KPIs
test_state: PipelineState = {
    "session_id": "test-blocking",
    "dataset": {"filename": "test.csv", "columns": ["a", "b"], "shape": [10, 2]},
    "dashboard_plan": {
        "name": "Test",
        "kpis": [
            {"name": "Revenue", "source_column": "sales", "aggregation": "sum"},
            {"name": "Total Records", "source_column": None, "aggregation": "count"},
            {
                "name": "Unique Products",
                "source_column": "product",
                "aggregation": "nunique",
            },
        ],
    },
    "shared_context": {},
    "query_results": [],
    "prepared_data": [
        {
            "kpi": "Revenue",
            "type": "timeseries",
            "data": [{"month": "Jan", "value": 100}, {"month": "Feb", "value": 200}],
        },
        {
            "kpi": "Total Records",
            "type": "timeseries",
            "data": [{"month": "Jan", "value": 50}, {"month": "Feb", "value": 50}],
        },
        {
            "kpi": "Unique Products",
            "type": "timeseries",
            "data": [{"category": "A", "value": 5}, {"category": "B", "value": 3}],
        },
    ],
    "transformed_data": [
        {"kpi": "Revenue", "min": 100, "max": 200, "points": 2},
        {"kpi": "Total Records", "min": 50, "max": 50, "points": 2},  # count
        {"kpi": "Unique Products", "min": 3, "max": 5, "points": 2},  # nunique
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
    "run_id": "test-blocking-001",
    "parent_run_id": None,
}

print("Testing count/nunique KPI blocking...")
print(f"Dashboard plan has these aggregations:")
for k in test_state["dashboard_plan"]["kpis"]:
    print(f"  - {k['name']}: {k['aggregation']}")

print(f"\nPrepared data KPIs:")
for p in test_state["prepared_data"]:
    print(f"  - {p['kpi']}")

result = chart_node(test_state)

print(f"\nCharts generated: {len(result['chart_specs'])}")
for chart in result["chart_specs"]:
    print(f"  - {chart['kpi']}: {chart['type']}")

# Verify only Revenue got a chart (not count/nunique)
chart_kpis = {c["kpi"] for c in result["chart_specs"]}
if (
    "Revenue" in chart_kpis
    and "Total Records" not in chart_kpis
    and "Unique Products" not in chart_kpis
):
    print("\n[OK] SUCCESS: Count and nunique KPIs were blocked!")
    sys.exit(0)
else:
    print(f"\n[FAIL] Expected only Revenue, got: {chart_kpis}")
    sys.exit(1)
