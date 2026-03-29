"""
Test Phase 0B - Dataset Intelligence
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000"

def test_phase_0b_intelligence():
    """Test complete Phase 0B pipeline"""
    print("\n" + "="*70)
    print("  PHASE 0B TEST: Dataset Intelligence")
    print("="*70)
    
    # Step 1: Upload CSV
    print("\n[Step 1] Uploading CSV...")
    with open("../data/test_data.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    assert response.status_code == 200
    upload_data = response.json()
    session_id = upload_data['session_id']
    print(f"✓ Session created: {session_id}")
    print(f"✓ Dataset shape: {upload_data['dataset']['shape']}")
    print(f"✓ Columns: {', '.join(upload_data['dataset']['columns'])}")
    
    # Wait for server to stabilize (in case of auto-reload)
    print("\n[Waiting] Allowing server to stabilize...")
    time.sleep(2)
    
    # Step 2: Generate Intelligence
    print("\n[Step 2] Generating dashboard intelligence...")
    response = requests.post(f"{BASE_URL}/intelligence/{session_id}")
    
    print(f"Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.json()}")
    
    assert response.status_code == 200
    intelligence = response.json()
    
    print(f"\n✓ Intelligence generated successfully!")
    print(f"\nKPIs ({len(intelligence['kpis'])}):")
    for i, kpi in enumerate(intelligence['kpis'], 1):
        print(f"  {i}. {kpi['name']}")
        print(f"     - Column: {kpi['source_column']}")
        print(f"     - Aggregation: {kpi['aggregation']}")
        if kpi['segment_by']:
            print(f"     - Segment by: {kpi['segment_by']}")
        if kpi['time_column']:
            print(f"     - Time column: {kpi['time_column']}")
        print(f"     - Meaning: {kpi['business_meaning']}")
        print(f"     - Confidence: {kpi['confidence']:.2f}")
    
    print(f"\nCharts ({len(intelligence['charts'])}):")
    for i, chart in enumerate(intelligence['charts'], 1):
        print(f"  {i}. {chart['title']}")
        print(f"     - Type: {chart['chart_type']}")
        print(f"     - X: {chart['x_column']}, Y: {chart['y_column']}")
    
    print(f"\nStory Arc:")
    print(f"  {intelligence['story_arc']}")
    
    print(f"\nKPI Coverage: {intelligence['kpi_coverage']:.1%}")
    
    # Validate response structure
    assert 'session_id' in intelligence
    assert 'kpis' in intelligence
    assert 'charts' in intelligence
    assert 'story_arc' in intelligence
    assert 'kpi_coverage' in intelligence
    
    # Validate KPIs
    assert len(intelligence['kpis']) >= 2, "Should have at least 2 KPIs"
    assert len(intelligence['kpis']) <= 3, "Should have at most 3 KPIs"
    for kpi in intelligence['kpis']:
        assert 'name' in kpi
        assert 'source_column' in kpi
        assert 'aggregation' in kpi
        assert 'business_meaning' in kpi
        assert 'confidence' in kpi
    
    # Validate Charts
    assert len(intelligence['charts']) >= 2, "Should have at least 2 charts"
    assert len(intelligence['charts']) <= 3, "Should have at most 3 charts"
    for chart in intelligence['charts']:
        assert 'chart_type' in chart
        assert 'title' in chart
        assert 'x_column' in chart
        assert 'y_column' in chart
        assert 'kpi_name' in chart
    
    print("\n" + "="*70)
    print("  ✓ PHASE 0B TEST PASSED!")
    print("="*70)
    
    return intelligence

if __name__ == "__main__":
    try:
        test_phase_0b_intelligence()
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Cannot connect to server")
        print("Please ensure the server is running:")
        print("  cd talking_bi")
        print("  uvicorn main:app --reload")
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
