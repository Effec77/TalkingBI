"""
Phase 3 end-to-end test script.
Run from talking_bi/ with venv active:
    python tests/run_phase3.py
"""
import sys
import json
import requests

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── 1: Health check ───────────────────────────────────────────
sep("STEP 1 - Health check")
r = requests.get(f"{BASE}/health")
print(f"Status : {r.status_code}")
assert r.status_code == 200

# ── 2: Upload CSV ─────────────────────────────────────────────
sep("STEP 2 - Upload CSV")
with open(CSV_PATH, "rb") as f:
    r = requests.post(f"{BASE}/upload", files={"file": ("test_data.csv", f, "text/csv")})
print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    sys.exit(1)
upload = r.json()
session_id = upload["session_id"]
print(f"session_id = {session_id}")
print(f"columns    = {upload['dataset']['columns']}")
print(f"shape      = {upload['dataset']['shape']}")

# ── 3: Run Phase 3 pipeline ──────────────────────────────────
sep("STEP 3 - POST /run/{session_id}")
r = requests.post(f"{BASE}/run/{session_id}")
print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    sys.exit(1)
result = r.json()

# ── 4: Results ────────────────────────────────────────────────
sep("STEP 4 - Results")

print(f"\n  run_id          : {result.get('run_id')}")
print(f"  errors          : {result.get('errors', [])}")
print(f"  execution_trace : {result.get('execution_trace', [])}")

print("\n-- _summary --")
summary = result.get("_summary", {})
for k, v in summary.items():
    print(f"  {k:20s}: {v}")

print("\n-- query_results --")
for qr in result.get("query_results", []):
    status = qr.get("status")
    kpi = qr.get("kpi")
    if status in ("success", "retry_success"):
        shape = qr.get("data_shape", qr.get("data", "scalar"))
        print(f"  [{status:14s}] {kpi} -> {shape}")
    else:
        print(f"  [{status:14s}] {kpi} -> {qr.get('error')}")

print("\n-- transformed_data (Phase 3 new) --")
for td in result.get("transformed_data", []):
    print(f"  {td['kpi']:20s}: min={td['min']:.2f}  max={td['max']:.2f}  points={td['points']}")

print("\n-- prepared_data --")
for pd_item in result.get("prepared_data", []):
    dtype = pd_item.get("type")
    kpi = pd_item.get("kpi")
    if dtype == "scalar":
        print(f"  {kpi:20s}: scalar = {pd_item.get('value')}")
    elif dtype == "timeseries":
        rows = pd_item.get("data", [])
        print(f"  {kpi:20s}: timeseries ({len(rows)} rows)")

print("\n-- insights --")
for ins in result.get("insights", []):
    if isinstance(ins, dict):
        print(f"  {ins.get('kpi', '?'):20s}: {ins.get('text', ins)}")
    else:
        print(f"  {ins}")

print("\n-- chart_specs --")
for cs in result.get("chart_specs", []):
    ct = cs.get("chart_type")
    kpi = cs.get("kpi")
    if ct == "line":
        print(f"  {kpi:20s}: line [x={cs.get('x_key')} y={cs.get('y_key')}]")
    elif ct == "metric":
        print(f"  {kpi:20s}: metric [{cs.get('value')}]")
    else:
        print(f"  {kpi:20s}: {ct}")

sep("DONE")
print(f"  query_results    : {len(result.get('query_results', []))}")
print(f"  transformed_data : {len(result.get('transformed_data', []))}")
print(f"  prepared_data    : {len(result.get('prepared_data', []))}")
print(f"  insights         : {len(result.get('insights', []))}")
print(f"  chart_specs      : {len(result.get('chart_specs', []))}")
print(f"  execution_trace  : {result.get('execution_trace', [])}")
print(f"  retries          : {summary.get('kpis_retried', 0)}")
print()
