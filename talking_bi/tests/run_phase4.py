"""
Phase 4 end-to-end test script.
Run from talking_bi/ with venv active:
    python tests/run_phase4.py
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

# ── 3: Run Phase 4 pipeline ──────────────────────────────────
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

print("\n-- transformed_data --")
for td in result.get("transformed_data", []):
    min_v = td['min'] if td['min'] is not None else 'N/A'
    max_v = td['max'] if td['max'] is not None else 'N/A'
    missing = td.get('missing', 0)
    print(f"  {td['kpi']:20s}: min={min_v}  max={max_v}  points={td['points']}  missing={missing}")

print("\n-- insights (Phase 4: structured multi-type) --")
for ins in result.get("insights", []):
    itype = ins.get("type", "?")
    kpi = ins.get("kpi", "?")
    conf = ins.get("confidence", 0)
    details = ins.get("details", {})
    print(f"  [{itype:14s}] {kpi:20s} conf={conf:.2f}  {json.dumps(details)}")

print("\n-- insight_summary (Phase 4: LLM narrative) --")
narrative = result.get("insight_summary")
if narrative:
    print(f"  {narrative}")
else:
    print("  (no narrative generated - LLM unavailable or skipped)")

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

# ── 5: Phase 4 Validation ────────────────────────────────────
sep("STEP 5 - Phase 4 Validation")

insights = result.get("insights", [])

# Check structured format
all_have_type = all("type" in i for i in insights)
all_have_conf = all("confidence" in i for i in insights)
all_have_details = all("details" in i for i in insights)

# Check insight types present
types_found = set(i["type"] for i in insights)

print(f"  all insights have 'type'       : {'PASS' if all_have_type else 'FAIL'}")
print(f"  all insights have 'confidence' : {'PASS' if all_have_conf else 'FAIL'}")
print(f"  all insights have 'details'    : {'PASS' if all_have_details else 'FAIL'}")
print(f"  insight types found            : {types_found}")
print(f"  has narrative                  : {'PASS' if narrative else 'SKIPPED (LLM)'}")

sep("DONE")
print(f"  query_results    : {len(result.get('query_results', []))}")
print(f"  transformed_data : {len(result.get('transformed_data', []))}")
print(f"  prepared_data    : {len(result.get('prepared_data', []))}")
print(f"  insights         : {len(insights)}")
print(f"  insight types    : {types_found}")
print(f"  insight_summary  : {'yes' if narrative else 'no'}")
print(f"  chart_specs      : {len(result.get('chart_specs', []))}")
print(f"  execution_trace  : {result.get('execution_trace', [])}")
print()
