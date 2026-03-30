"""
Phase 2 end-to-end test script.
Run from talking_bi/ directory with the venv activated:
    python tests/run_phase2.py
"""
import sys
import json
import requests

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def separator(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── STEP 1: Health check ───────────────────────────────────────
separator("STEP 1 — Health check")
r = requests.get(f"{BASE}/health")
print(f"Status : {r.status_code}")
print(f"Body   : {r.json()}")
assert r.status_code == 200, "Health check failed"


# ── STEP 2: Upload CSV ─────────────────────────────────────────
separator("STEP 2 — Upload CSV")
with open(CSV_PATH, "rb") as f:
    r = requests.post(f"{BASE}/upload", files={"file": ("test_data.csv", f, "text/csv")})

print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    sys.exit(1)

upload_resp = r.json()
print(json.dumps(upload_resp, indent=2))
session_id = upload_resp["session_id"]
print(f"\n✓ session_id = {session_id}")


# ── STEP 3: Run Phase 2 pipeline ───────────────────────────────
separator("STEP 3 — POST /run/{session_id}")
r = requests.post(f"{BASE}/run/{session_id}")
print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    sys.exit(1)

result = r.json()

# ── STEP 4: Inspect results ────────────────────────────────────
separator("STEP 4 — Results")

print(f"\n  run_id     : {result.get('run_id')}")
print(f"  session_id : {result.get('session_id')}")
print(f"  errors     : {result.get('errors', [])}")

print("\n── query_results ──────────────────────────────────────────")
for qr in result.get("query_results", []):
    status = qr.get("status")
    kpi    = qr.get("kpi")
    if status == "success":
        data = qr.get("data")
        print(f"  ✓ [{status}] {kpi}: {data}")
    else:
        print(f"  ✗ [{status}] {kpi}: {qr.get('error')}")

print("\n── prepared_data ──────────────────────────────────────────")
for pd_item in result.get("prepared_data", []) or []:
    dtype = pd_item.get("type")
    kpi   = pd_item.get("kpi")
    if dtype == "scalar":
        print(f"  • {kpi} → scalar: {pd_item.get('value')}")
    elif dtype == "timeseries":
        rows = pd_item.get("data", [])
        print(f"  • {kpi} → timeseries: {len(rows)} rows")
        if rows:
            print(f"    first row: {rows[0]}")

print("\n── insights ───────────────────────────────────────────────")
for ins in result.get("insights", []) or []:
    print(f"  💡 {ins.get('kpi')}: {ins.get('text')}")

print("\n── chart_specs ────────────────────────────────────────────")
for cs in result.get("chart_specs", []) or []:
    chart_type = cs.get("chart_type")
    kpi        = cs.get("kpi")
    if chart_type == "line":
        print(f"  📈 {kpi} → line  [x={cs.get('x_key')} y={cs.get('y_key')}]")
    elif chart_type == "metric":
        print(f"  🔢 {kpi} → metric [{cs.get('value')}]")
    else:
        print(f"  ⚠  {kpi} → {chart_type}")

separator("DONE")
print(f"  query_results : {len(result.get('query_results', []))}")
print(f"  prepared_data : {len(result.get('prepared_data') or [])}")
print(f"  insights      : {len(result.get('insights') or [])}")
print(f"  chart_specs   : {len(result.get('chart_specs') or [])}")
print()
