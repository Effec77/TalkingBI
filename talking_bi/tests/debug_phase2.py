"""
Phase 2 diagnostic — runs the pipeline locally (no HTTP) to get the full traceback.
Run from talking_bi/ with venv active:
    python tests/debug_phase2.py
"""
import sys
import traceback
import pandas as pd

# ── Bootstrap path so imports work ────────────────────────────
sys.path.insert(0, ".")

from dataclasses import asdict
from uuid import uuid4

print("=" * 60)
print("  Phase 2 Diagnostic")
print("=" * 60)

# ── Step 1: Load CSV directly ──────────────────────────────────
print("\n[1] Loading test CSV...")
df = pd.read_csv("data/test_data.csv")
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
print(f"    Shape: {df.shape}")
print(f"    Columns: {list(df.columns)}")
print(f"    dtypes:\n{df.dtypes}\n")

# ── Step 2: Build dashboard plan ───────────────────────────────
print("[2] Building dashboard plan (Phase 0B)...")
try:
    from models.contracts import UploadedDataset
    from services.intelligence_engine import generate_dashboard_plan

    session_id = str(uuid4())
    metadata = UploadedDataset(
        session_id=session_id,
        filename="test_data.csv",
        columns=list(df.columns),
        dtypes={col: str(df[col].dtype) for col in df.columns},
        shape=df.shape,
        sample_values={col: df[col].dropna().astype(str).unique()[:3].tolist() for col in df.columns},
        missing_pct={col: float(df[col].isna().mean()) for col in df.columns},
    )

    plan = generate_dashboard_plan(
        session_id=session_id, df=df, uploaded_dataset=metadata
    )
    print(f"    Plan type: {type(plan)}")
    print(f"    KPIs: {[k.name for k in plan.kpis]}")
    print(f"    Charts: {len(plan.charts)}")
except Exception:
    print("  ✗ FAILED at dashboard plan:")
    traceback.print_exc()
    sys.exit(1)

# ── Step 3: asdict(plan) ───────────────────────────────────────
print("\n[3] Converting plan to dict via asdict()...")
try:
    plan_dict = asdict(plan)
    import json
    # Test JSON serialisability of the plan dict
    json.dumps(plan_dict)
    plan_dict["_meta"] = {
        "kpi_count": len(plan.kpis),
        "chart_count": len(plan.charts),
    }
    print(f"    ✓ asdict() OK — kpis={plan_dict['_meta']['kpi_count']}")
    print(f"    KPI names: {[k['name'] for k in plan_dict['kpis']]}")
    print(f"    Aggs: {[k['aggregation'] for k in plan_dict['kpis']]}")
except Exception:
    print("  ✗ FAILED at asdict / json serialisation:")
    traceback.print_exc()
    sys.exit(1)

# ── Step 4: Build initial state ────────────────────────────────
print("\n[4] Building initial state...")
run_id = str(uuid4())
initial_state = {
    "session_id": session_id,
    "dataset": {
        "filename": metadata.filename,
        "columns": metadata.columns,
        "shape": list(metadata.shape),
        "dtypes": metadata.dtypes,
        "missing_pct": metadata.missing_pct,
    },
    "dashboard_plan": {**plan_dict},
    "shared_context": {"applied_filters": [], "run_id": run_id},
    "query_results": [],
    "prepared_data": None,
    "insights": [],
    "chart_specs": [],
    "is_refinement": False,
    "target_components": [],
    "retry_count": 0,
    "errors": [],
    "run_id": run_id,
    "parent_run_id": None,
}
print(f"    ✓ run_id = {run_id}")

# ── Step 5: Register df and run pipeline ───────────────────────
print("\n[5] Running pipeline...")
try:
    from graph.df_registry import register_df, deregister_df
    from graph.executor import run_pipeline

    register_df(run_id, df)
    try:
        result = run_pipeline(initial_state)
    finally:
        deregister_df(run_id)

    print("\n  ✓ Pipeline complete!")
    print(f"    query_results : {len(result.get('query_results', []))}")
    print(f"    prepared_data : {len(result.get('prepared_data') or [])}")
    print(f"    insights      : {len(result.get('insights') or [])}")
    print(f"    chart_specs   : {len(result.get('chart_specs') or [])}")
    print(f"    errors        : {result.get('errors', [])}")

    print("\n  query_results detail:")
    for qr in result.get("query_results", []):
        print(f"    [{qr['status']}] {qr['kpi']} → {qr.get('data', qr.get('error'))}")

    print("\n  insights:")
    for ins in result.get("insights") or []:
        print(f"    💡 {ins.get('text')}")

    print("\n  chart_specs:")
    for cs in result.get("chart_specs") or []:
        print(f"    {cs.get('chart_type')} — {cs.get('kpi')}")

except Exception:
    print("  ✗ FAILED during pipeline execution:")
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("  ALL STEPS PASSED")
print("=" * 60)
