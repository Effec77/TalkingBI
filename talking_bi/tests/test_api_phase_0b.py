"""
Test Phase 0B API Integration
Tests the complete flow: Upload → Intelligence → Dashboard Plan
"""
import requests
import json

BASE_URL = "http://localhost:8000"


def test_complete_phase_0b_api():
    """Test complete Phase 0B flow via API"""
    print("\n" + "="*70)
    print("  PHASE 0B API INTEGRATION TEST")
    print("  Testing: Upload → Intelligence → Dashboard Plan")
    print("="*70)
    
    # Step 1: Upload CSV
    print("\n[STEP 1] Uploading test_data.csv...")
    with open("data/test_data.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    assert response.status_code == 200, f"Upload failed: {response.text}"
    upload_data = response.json()
    session_id = upload_data["session_id"]
    print(f"✓ Upload successful: session_id={session_id}")
    print(f"  - Filename: {upload_data.get('filename', 'N/A')}")
    print(f"  - Columns: {len(upload_data.get('columns', []))}")
    
    # Step 2: Generate Dashboard Plan
    print(f"\n[STEP 2] Generating dashboard plan for session {session_id}...")
    response = requests.post(f"{BASE_URL}/intelligence/{session_id}")
    
    assert response.status_code == 200, f"Intelligence failed: {response.text}"
    plan_data = response.json()
    
    print(f"✓ Dashboard plan generated successfully")
    print(f"  - KPIs: {len(plan_data['kpis'])}")
    print(f"  - Charts: {len(plan_data['charts'])}")
    print(f"  - Coverage: {plan_data['kpi_coverage']:.1%}")
    
    # Step 3: Validate Results
    print(f"\n[STEP 3] Validating results...")
    assert len(plan_data['kpis']) == 3, f"Expected 3 KPIs, got {len(plan_data['kpis'])}"
    assert len(plan_data['charts']) == 3, f"Expected 3 charts, got {len(plan_data['charts'])}"
    assert plan_data['kpi_coverage'] == 1.0, f"Expected 100% coverage, got {plan_data['kpi_coverage']:.1%}"
    
    print(f"\n[KPIs Generated]:")
    for i, kpi in enumerate(plan_data['kpis'], 1):
        print(f"  {i}. {kpi['name']}")
        print(f"     - Source: {kpi['source_column']}")
        print(f"     - Aggregation: {kpi['aggregation']}")
        print(f"     - Meaning: {kpi['business_meaning']}")
    
    print(f"\n[Charts Generated]:")
    for i, chart in enumerate(plan_data['charts'], 1):
        print(f"  {i}. {chart['title']}")
        print(f"     - Type: {chart['chart_type']}")
        print(f"     - X: {chart['x_column']}, Y: {chart['y_column']}")
    
    print(f"\n[Story Arc]:")
    print(f"  {plan_data['story_arc']}")
    
    print("\n" + "="*70)
    print("  ✓ ALL API TESTS PASSED")
    print("  Phase 0B Multi-Provider Orchestration: WORKING VIA API")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        test_complete_phase_0b_api()
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}\n")
        exit(1)
    except requests.exceptions.ConnectionError:
        print(f"\n✗ ERROR: Cannot connect to server at {BASE_URL}")
        print("  Make sure the server is running: uvicorn main:app --reload\n")
        exit(1)
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        exit(1)
