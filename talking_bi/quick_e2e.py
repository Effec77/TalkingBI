"""
Quick E2E Test - First 3 flows only
"""

import sys

sys.path.insert(0, ".")

import asyncio
import pandas as pd
from dataclasses import dataclass
from api.query import query_endpoint, QueryPayload
from services.session_manager import create_session, delete_session
from services.conversation_manager import get_conversation_manager


@dataclass
class MockDataset:
    filename: str
    columns: list
    shape: tuple


results = {"ok": 0, "fail": 0}


async def test_flow():
    print("Loading ecommerce dataset...")
    df = pd.read_csv(r"D:\datasets for TalkingBI\ecommerce.csv")
    session_id = create_session(
        df, MockDataset("ecommerce.csv", list(df.columns), df.shape)
    )
    print(f"Session: {session_id}")

    conv_mgr = get_conversation_manager()

    test_cases = [
        ("Flow 1 T1", "show revenue", "RESOLVED"),
        ("Flow 1 T2", "by region", "RESOLVED"),
        ("Flow 2 T1", "revenue numbers", "RESOLVED"),
        ("Flow 3 T2", "filter null product category", None),  # Should not crash
    ]

    for flow, query, expected in test_cases:
        print(f"\n[{flow}] '{query}'")
        try:
            response = await query_endpoint(session_id, QueryPayload(query=query))
            status = response.get("status")
            charts = len(response.get("charts", []))

            if expected and status == expected:
                print(f"  [OK] Status={status}, Charts={charts}")
                results["ok"] += 1
            elif not expected and status != "ERROR":
                print(f"  [OK] Status={status} (no crash)")
                results["ok"] += 1
            else:
                print(f"  [FAIL] Expected {expected}, got {status}")
                results["fail"] += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            results["fail"] += 1

    # Cleanup
    conv_mgr.clear_session(session_id)
    try:
        delete_session(session_id)
    except:
        pass

    print(f"\n=== RESULTS: {results['ok']} OK, {results['fail']} FAIL ===")
    return results


if __name__ == "__main__":
    asyncio.run(test_flow())
