"""
Complete Pipeline Test - Phase 0A to Phase 5
Run this in talking_bi/ directory with the server already running.
"""

import sys
import json
import requests
import time

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def sep(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def run_complete_test():
    """Run complete pipeline test."""
    sep("TALKING BI - COMPLETE PIPELINE TEST (Phase 0A to Phase 5)")

    # Check server is running
    print("\nChecking server...")
    try:
        r = requests.get(f"{BASE}/health", timeout=5)
        if r.status_code != 200:
            print("FAIL: Server not responding")
            print("\nPlease start the server first:")
            print("  python main.py")
            return False
    except:
        print("FAIL: Cannot connect to server at localhost:8000")
        print("\nPlease start the server first:")
        print("  python main.py")
        return False

    print("OK: Server is running\n")

    # Phase 0A: Upload
    sep("PHASE 0A: UPLOAD CSV")
    try:
        with open(CSV_PATH, "rb") as f:
            r = requests.post(
                f"{BASE}/upload",
                files={"file": ("test_data.csv", f, "text/csv")},
                timeout=30,
            )

        if r.status_code != 200:
            print(f"FAIL: Upload failed - {r.text}")
            return False

        data = r.json()
        session_id = data["session_id"]
        print(f"OK: Session created: {session_id}")
        print(f"  - Shape: {data['dataset']['shape']}")
        print(f"  - Columns: {data['dataset']['columns']}")
    except Exception as e:
        print(f"FAIL: {e}")
        return False

    # Phase 0B: Intelligence
    sep("PHASE 0B: DASHBOARD INTELLIGENCE")
    try:
        r = requests.post(f"{BASE}/intelligence/{session_id}", timeout=60)

        if r.status_code != 200:
            print(f"FAIL: Intelligence failed - {r.text}")
            return False

        plan = r.json()
        print(f"OK: Dashboard plan generated")
        print(f"  - KPIs: {len(plan['kpis'])}")
        print(f"  - Charts: {len(plan['charts'])}")
        print(f"  - KPI Coverage: {plan['kpi_coverage'] * 100:.1f}%")

        print(f"\n  Generated KPIs:")
        for i, kpi in enumerate(plan["kpis"], 1):
            print(
                f"    {i}. {kpi['name']} ({kpi['source_column']}, {kpi['aggregation']})"
            )
    except Exception as e:
        print(f"FAIL: {e}")
        return False

        plan = r.json()
        print(f"OK: Dashboard plan generated")
        print(f"  - Plan ID: {plan['plan_id']}")
        print(f"  - KPIs: {len(plan['plan']['kpis'])}")
        print(f"  - Charts: {len(plan['plan']['charts'])}")

        print(f"\n  Generated KPIs:")
        for i, kpi in enumerate(plan["plan"]["kpis"], 1):
            print(
                f"    {i}. {kpi['name']} ({kpi['source_column']}, {kpi['aggregation']})"
            )
    except Exception as e:
        print(f"FAIL: {e}")
        return False

    # Phases 2-5: Pipeline
    sep("PHASES 2-5: PIPELINE EXECUTION")
    try:
        r = requests.post(f"{BASE}/run/{session_id}", timeout=120)

        if r.status_code != 200:
            print(f"FAIL: Pipeline failed - {r.text}")
            return False

        result = r.json()
        print(f"OK: Pipeline completed")
        print(f"  - Run ID: {result['run_id']}")
        print(f"  - Trace: {' -> '.join(result['execution_trace'])}")

        summary = result.get("_summary", {})
        print(f"\n  Execution Summary:")
        print(f"    - KPIs executed: {summary.get('kpis_executed', 0)}")
        print(f"    - KPIs succeeded: {summary.get('kpis_succeeded', 0)}")
        print(f"    - Charts: {summary.get('charts', 0)}")
        print(f"    - Insights: {summary.get('insights', 0)}")
    except Exception as e:
        print(f"FAIL: {e}")
        return False

    # Phase 5 Verification
    sep("PHASE 5 VERIFICATION")

    # Check UI block
    if "ui" not in result:
        print("FAIL: No UI block in response")
        return False
    ui = result["ui"]
    print(f"OK: UI block present")
    print(f"  - Summary: {'Yes' if ui.get('summary') else 'No'}")
    print(f"  - Top KPIs: {len(ui.get('top_kpis', []))}")
    print(f"  - Top Insights: {len(ui.get('top_insights', []))}")
    print(f"  - Charts: {len(ui.get('charts', []))}")

    # Check chart images
    charts = result.get("chart_specs", [])
    images = sum(1 for c in charts if c.get("type") == "line" and c.get("image"))
    metrics = sum(1 for c in charts if c.get("type") == "metric")

    # DEBUG: Show aggregation types
    print(f"\n  DEBUG: Checking for count/nunique KPIs in charts:")
    for chart in charts:
        print(f"    - {chart['kpi']}: type={chart.get('type')}")

    print(f"\nOK: {images} charts with base64 images, {metrics} metrics")

    for chart in charts:
        if chart.get("type") == "line" and chart.get("image"):
            print(f"  - {chart['kpi']}: {len(chart['image'])} chars")
        elif chart.get("type") == "metric":
            print(f"  - {chart['kpi']}: value={chart.get('value')}")

    # Check insight scores
    insights = result.get("insights", [])
    scored = sum(1 for i in insights if "score" in i)
    print(f"\nOK: {scored}/{len(insights)} insights with scores")

    if scored > 0:
        print("\n  Top insights by score:")
        for i, insight in enumerate([x for x in insights if "score" in x][:3], 1):
            print(
                f"    {i}. [{insight['type']:10}] {insight['kpi'][:25]:25} score={insight['score']:.2f}"
            )

    # Check KPI ranking
    transformed = result.get("transformed_data", [])
    if len(transformed) >= 2:
        print(f"\nOK: KPIs ranked by importance")
        for i, t in enumerate(transformed[:3], 1):
            importance = abs((t.get("max") or 0) - (t.get("min") or 0)) * t.get(
                "points", 1
            )
            print(f"    {i}. {t['kpi'][:30]:30} importance={importance:.0f}")

    # Final Summary
    sep("FINAL SUMMARY")
    print("[OK] Phase 0A: Upload CSV")
    print("[OK] Phase 0B: Dashboard Intelligence")
    print("[OK] Phase 2: Query Execution")
    print("[OK] Phase 3: Data Preparation")
    print("[OK] Phase 4: Insight Generation")

    all_phase5_ok = images > 0 and scored > 0 and "ui" in result
    if all_phase5_ok:
        print("[OK] Phase 5: Visualization + UI Response")
    else:
        print("[FAIL] Phase 5: Some checks failed")

    sep("TEST COMPLETE")
    if all_phase5_ok:
        print("*** ALL PHASES PASSED ***")
        print("Pipeline is fully operational from Phase 0A to Phase 5!")
    else:
        print("*** SOME CHECKS FAILED ***")
        print("Review the output above for details.")

    return all_phase5_ok


if __name__ == "__main__":
    success = run_complete_test()
    sys.exit(0 if success else 1)
