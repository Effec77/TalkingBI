"""
Test script for Talking BI Phase 0A API
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    print("\n=== Testing Health Endpoint ===")
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    assert response.status_code == 200
    print("✓ Health check passed")

def test_valid_upload():
    """Test valid CSV upload"""
    print("\n=== Testing Valid CSV Upload ===")
    with open("../data/test_data.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2)}")
    
    assert response.status_code == 200
    assert "session_id" in data
    assert "dataset" in data
    assert data["dataset"]["shape"] == [10, 5]
    assert "sales" in data["dataset"]["columns"]
    assert data["dataset"]["missing_pct"]["sales"] == 0.1
    print("✓ Valid upload test passed")
    return data["session_id"]

def test_invalid_file_type():
    """Test invalid file type"""
    print("\n=== Testing Invalid File Type ===")
    # Create a temporary test file
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
        tmp.write(b"This is not a CSV")
        tmp_path = tmp.name
    
    with open(tmp_path, "rb") as f:
        files = {"file": ("test.txt", f, "text/plain")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]
    print("✓ Invalid file type test passed")
    
    # Cleanup
    import os
    os.unlink(tmp_path)

def test_empty_csv():
    """Test empty CSV"""
    print("\n=== Testing Empty CSV ===")
    with open("../data/empty.csv", "rb") as f:
        files = {"file": ("empty.csv", f, "text/csv")}
        response = requests.post(f"{BASE_URL}/upload", files=files)
    
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    assert response.status_code == 400
    print("✓ Empty CSV test passed")

if __name__ == "__main__":
    try:
        test_health()
        session_id = test_valid_upload()
        test_invalid_file_type()
        test_empty_csv()
        
        print("\n" + "="*50)
        print("✓ ALL TESTS PASSED!")
        print("="*50)
        print(f"\nSession created: {session_id}")
        print("Session will expire in 24 hours")
        print("Cleanup runs every 10 minutes")
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        raise
