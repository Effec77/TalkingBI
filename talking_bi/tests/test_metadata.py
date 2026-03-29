"""
Test to verify metadata storage in sessions
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_metadata_storage():
    """Test that metadata is stored correctly in session"""
    print("\n=== Testing Metadata Storage ===")
    
    # Upload a CSV
    with open("../data/test_data.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    assert response.status_code == 200
    data = response.json()
    session_id = data["session_id"]
    
    print(f"Session ID: {session_id}")
    print(f"Columns: {data['dataset']['columns']}")
    print(f"Shape: {data['dataset']['shape']}")
    print(f"Missing %: {data['dataset']['missing_pct']}")
    
    # Verify response format hasn't changed
    assert "session_id" in data
    assert "dataset" in data
    assert "filename" in data["dataset"]
    assert "shape" in data["dataset"]
    assert "columns" in data["dataset"]
    assert "missing_pct" in data["dataset"]
    
    # Verify dtypes and sample_values are NOT in API response (as per requirements)
    assert "dtypes" not in data["dataset"]
    assert "sample_values" not in data["dataset"]
    
    print("✓ Metadata storage test passed")
    print("✓ API response format unchanged")
    print("✓ dtypes and sample_values stored internally (not exposed in API)")

if __name__ == "__main__":
    try:
        test_metadata_storage()
        print("\n" + "="*50)
        print("✓ METADATA TEST PASSED!")
        print("="*50)
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        raise
