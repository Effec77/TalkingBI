"""
Complete Pipeline Test - Phase 0A to Phase 5 (Background Server Version)
"""

import sys
import os
import json
import time
import subprocess
import requests

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def sep(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def start_server_bg():
    """Start server in background."""
    # Kill existing
    subprocess.run(["taskkill", "/F", "/IM", "python.exe"], capture_output=True)
    time.sleep(2)

    # Start new server
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import uvicorn; uvicorn.run('main:app', host='0.0.0.0', port=8000, log_level='warning')",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(os.path.abspath(__file__)) + "/..",
    )

    # Wait for startup
    for i in range(30):
        try:
            if requests.get(f"{BASE}/health", timeout=1).status_code == 200:
                print(f"Server ready (attempt {i + 1})")
                return proc
        except:
            pass
        time.sleep(0.5)

    return None


def run_test():
    sep("PHASE 0A: UPLOAD CSV")

    # Upload
    with open(CSV_PATH, "rb") as f:
        r = requests.post(
            f"{BASE}/upload", files={"file": ("test.csv", f, "text/csv")}, timeout=30
        )

    if r.status_code != 200:
        print(f"FAIL: Upload failed - {r.text}")
        return False

    session_id = r.json()["session_id"]
    print(f"OK: Session {session_id}")

    sep("PHASE 0B: INTELLIGENCE")

    r = requests.post(f"{BASE}/intelligence/{session_id}", timeout=60)
    if r.status_code != 200:
        print(f"FAIL: Intelligence failed - {r.text}")
        return False

    plan = r.json()
    print(f"OK: Plan with {len(plan['plan']['kpis'])} KPIs")

    sep("PHASES 2-5: PIPELINE")

    r = requests.post(f"{BASE}/run/{session_id}", timeout=120)
    if r.status_code != 200:
        print(f"FAIL: Pipeline failed - {r.text}")
        return False

    result = r.json()
    print(f"OK: Pipeline completed")
    print(f"  Trace: {' -> '.join(result['execution_trace'])}")

    # Phase 5 checks
    sep("PHASE 5 VERIFICATION")

    # UI block
    if "ui" not in result:
        print("FAIL: No UI block")
        return False
    print(f"OK: UI block present")

    # Chart images
    charts = result.get("chart_specs", [])
    images = sum(1 for c in charts if c.get("type") == "line" and c.get("image"))
    print(f"OK: {images} charts with base64 images")

    # Insight scores
    insights = result.get("insights", [])
    scored = sum(1 for i in insights if "score" in i)
    print(f"OK: {scored}/{len(insights)} insights with scores")

    # Summary
    sep("FINAL RESULT")
    all_ok = images > 0 and scored > 0 and "ui" in result
    if all_ok:
        print("SUCCESS: All phases passed!")
    else:
        print("FAILED: Some checks did not pass")

    return all_ok


# Main
sep("PHASE 0A TO 5 - COMPLETE TEST")

server = start_server_bg()
if not server:
    print("FAIL: Could not start server")
    sys.exit(1)

try:
    success = run_test()
finally:
    server.terminate()
    try:
        server.wait(timeout=5)
    except:
        subprocess.run(["taskkill", "/F", "/IM", "python.exe"], capture_output=True)

sys.exit(0 if success else 1)
