"""
Test Phase 6A: Conversation-based query endpoint
"""

import sys
import requests

BASE = "http://localhost:8000"
CSV_PATH = "data/test_data.csv"


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def test_phase6a():
    sep("PHASE 6A TEST: Conversation Query")

    # Step 1: Health check
    print("\n1. Health check...")
    r = requests.get(f"{BASE}/health")
    if r.status_code != 200:
        print("FAIL: Server not running")
        return False
    print("OK: Server is healthy")

    # Step 2: Upload CSV
    print("\n2. Upload CSV...")
    with open(CSV_PATH, "rb") as f:
        r = requests.post(f"{BASE}/upload", files={"file": ("test.csv", f, "text/csv")})
    if r.status_code != 200:
        print(f"FAIL: Upload failed: {r.text}")
        return False
    session_id = r.json()["session_id"]
    print(f"OK: Session {session_id}")

    # Step 3: First query
    print("\n3. First query...")
    r = requests.post(
        f"{BASE}/query/{session_id}", json={"query": "Show me revenue trends"}
    )
    if r.status_code != 200:
        print(f"FAIL: Query failed: {r.text}")
        return False
    result1 = r.json()
    print(f"OK: Query executed")
    print(f"  - Run ID: {result1['run_id']}")
    print(f"  - Turn count: {result1['conversation']['turn_count']}")
    print(f"  - History length: {result1['conversation']['history_length']}")

    # Step 4: Second query (session should persist)
    print("\n4. Second query...")
    r = requests.post(
        f"{BASE}/query/{session_id}", json={"query": "Break it down by region"}
    )
    if r.status_code != 200:
        print(f"FAIL: Second query failed: {r.text}")
        return False
    result2 = r.json()
    print(f"OK: Second query executed")
    print(f"  - Run ID: {result2['run_id']}")
    print(f"  - Turn count: {result2['conversation']['turn_count']}")
    print(f"  - History length: {result2['conversation']['history_length']}")

    # Verify turn count increased
    if result2["conversation"]["turn_count"] != 2:
        print(
            f"FAIL: Expected turn_count=2, got {result2['conversation']['turn_count']}"
        )
        return False
    print("OK: Turn count incremented correctly")

    # Step 5: Get conversation history
    print("\n5. Get conversation history...")
    r = requests.get(f"{BASE}/query/{session_id}/history")
    if r.status_code != 200:
        print(f"FAIL: History endpoint failed: {r.text}")
        return False
    history = r.json()
    print(f"OK: Retrieved history")
    print(f"  - Total turns: {len(history['turns'])}")
    for turn in history["turns"]:
        print(f"    Turn {turn['turn']}: {turn['query'][:50]}...")

    # Verify backward compatibility with /run
    print("\n6. Verify /run still works (backward compatibility)...")
    r = requests.post(f"{BASE}/run/{session_id}")
    if r.status_code != 200:
        print(f"FAIL: /run endpoint failed: {r.text}")
        return False
    print("OK: /run endpoint still works")

    sep("PHASE 6A: ALL TESTS PASSED")
    print("\nFeatures verified:")
    print("  [OK] /query endpoint accepts natural language")
    print("  [OK] Session state persists across queries")
    print("  [OK] Conversation history tracked")
    print("  [OK] Turn count increments correctly")
    print("  [OK] /run endpoint maintains backward compatibility")
    print("  [OK] Full pipeline execution works")

    return True


if __name__ == "__main__":
    success = test_phase6a()
    sys.exit(0 if success else 1)
