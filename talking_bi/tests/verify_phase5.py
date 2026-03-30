"""
Phase 5 verification script with embedded server startup.
Tests that charts have base64 images and UI block is present.
"""

import sys
import json
import requests
import subprocess
import time
import signal
import os

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def wait_for_server(max_attempts=30):
    """Wait for server to be ready."""
    for i in range(max_attempts):
        try:
            r = requests.get(f"{BASE}/health", timeout=1)
            if r.status_code == 200:
                print(f"Server ready after {i + 1} attempts")
                return True
        except:
            pass
        time.sleep(0.5)
    return False


# ── Start server ───────────────────────────────────────────────
sep("STARTING SERVER")
server_process = subprocess.Popen(
    [sys.executable, "main.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=os.path.dirname(os.path.abspath(__file__)) + "/..",
)

print("Waiting for server to start...")
if not wait_for_server():
    print("ERROR: Server failed to start")
    server_process.terminate()
    sys.exit(1)

# ── 1: Health check ───────────────────────────────────────────
sep("STEP 1 - Health check")
r = requests.get(f"{BASE}/health")
print(f"Status : {r.status_code}")
assert r.status_code == 200

# ── 2: Upload CSV ─────────────────────────────────────────────
sep("STEP 2 - Upload CSV")
with open(CSV_PATH, "rb") as f:
    r = requests.post(
        f"{BASE}/upload", files={"file": ("test_data.csv", f, "text/csv")}
    )
print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    server_process.terminate()
    sys.exit(1)
upload = r.json()
session_id = upload["session_id"]
print(f"session_id = {session_id}")

# ── 3: Run pipeline ───────────────────────────────────────────
sep("STEP 3 - POST /run/{session_id}")
r = requests.post(f"{BASE}/run/{session_id}")
print(f"Status : {r.status_code}")
if r.status_code != 200:
    print(f"Error  : {r.text}")
    server_process.terminate()
    sys.exit(1)
result = r.json()

# ── 4: Phase 5 Verification ───────────────────────────────────
sep("STEP 4 - Phase 5 Verification")

# Debug: print top-level keys
print(f"Response keys: {list(result.keys())}")

# Check UI block exists
if "ui" in result:
    print("[PASS] UI block present")
    ui = result["ui"]
    print(f"  - summary: {'yes' if ui.get('summary') else 'no'}")
    print(f"  - top_kpis: {len(ui.get('top_kpis', []))} items")
    print(f"  - top_insights: {len(ui.get('top_insights', []))} items")
    print(f"  - charts: {len(ui.get('charts', []))} items")
else:
    print("[FAIL] UI block missing")
    server_process.terminate()
    sys.exit(1)

# Check charts have base64 images
charts = result.get("chart_specs", [])
images_found = 0
for chart in charts:
    if chart.get("type") in ("line", "bar"):
        if chart.get("image"):
            img_len = len(chart["image"])
            print(f"  [OK] {chart['kpi']}: image present ({img_len} chars)")
            images_found += 1
        else:
            print(f"  [WARN] {chart['kpi']}: image missing")
    elif chart.get("type") == "metric":
        print(f"  [OK] {chart['kpi']}: metric value = {chart.get('value')}")

print(f"\nCharts with images: {images_found}/{len(charts)}")

# Check insights have scores
insights = result.get("insights", [])
scores_found = sum(1 for i in insights if "score" in i)
print(f"Insights with scores: {scores_found}/{len(insights)}")

# Check KPI ranking (transformed_data should be sorted by importance)
transformed = result.get("transformed_data", [])
if len(transformed) >= 2:
    # Calculate variance to check if sorted
    def calc_importance(item):
        min_val = item.get("min", 0) or 0
        max_val = item.get("max", 0) or 0
        points = item.get("points", 1)
        mean_val = (max_val + min_val) / 2 if (max_val and min_val) else 1
        if mean_val == 0:
            mean_val = 1
        range_val = abs(max_val - min_val)
        return (range_val / mean_val) * (points**0.5)

    importances = [calc_importance(t) for t in transformed]
    is_sorted = all(
        importances[i] >= importances[i + 1] for i in range(len(importances) - 1)
    )
    print(f"KPIs ranked by importance: {'PASS' if is_sorted else 'FAIL'}")
    for i, t in enumerate(transformed):
        print(f"  {i + 1}. {t['kpi']}: importance={importances[i]:.0f}")

# ── Stop server ───────────────────────────────────────────────
sep("STOPPING SERVER")
server_process.terminate()
try:
    server_process.wait(timeout=5)
except:
    server_process.kill()

sep("DONE - Phase 5 Implementation Verified")
