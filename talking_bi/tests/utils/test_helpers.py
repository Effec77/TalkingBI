"""
Test Helpers for Phase 6B Runtime Validation
Provides utilities to test actual system behavior, not code structure.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from io import BytesIO
from dataclasses import dataclass, field
from services.session_manager import create_session, get_session
from services.conversation_manager import get_conversation_manager
from api.query import query_endpoint, QueryPayload
from fastapi import HTTPException
import asyncio


def create_test_session():
    """Create a test session with sample data for runtime testing."""
    # Create sample DataFrame
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "sales": [100, 120, 90, 150, 180, 200, 190, 220, 250, 230],
            "region": ["North"] * 5 + ["South"] * 5,
            "product": ["A", "B"] * 5,
            "quantity": [10, 12, 9, 15, 18, 20, 19, 22, 25, 23],
        }
    )

    # Create upload session
    from dataclasses import dataclass

    @dataclass
    class MockMetadata:
        filename: str = "test.csv"
        columns: list = field(default_factory=list)
        shape: tuple = (10, 5)

        def __post_init__(self):
            if not self.columns:
                self.columns = list(df.columns)

    metadata = MockMetadata()
    session_id = create_session(df, metadata=metadata)

    return session_id


def simulate_query_sync(session_id: str, query_text: str):
    """
    Synchronously simulate a query to the /query endpoint.

    Returns the full response dict for runtime validation.
    """
    # Create event loop for async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Call the query endpoint
        payload = QueryPayload(query=query_text)
        response = loop.run_until_complete(query_endpoint(session_id, payload))

        # Response is already a dict from FastAPI
        return response
    except HTTPException as e:
        # Return error response structure
        return {
            "session_id": session_id,
            "run_id": None,
            "query": query_text,
            "status": "ERROR",
            "error": e.detail,
            "intent": {
                "intent": "UNKNOWN",
                "kpi": None,
                "dimension": None,
                "filter": None,
            },
        }
    finally:
        loop.close()


def cleanup_test_session(session_id: str):
    """Clean up test session and conversation state."""
    # Clear conversation session
    conv_manager = get_conversation_manager()
    conv_manager.clear_session(session_id)


def validate_intent_in_response(response: dict) -> bool:
    """Validate that intent is properly structured in response."""
    if "intent" not in response:
        return False

    intent = response["intent"]
    required_fields = ["intent", "kpi", "dimension", "filter"]

    return all(field in intent for field in required_fields)


def validate_intent_in_state(response: dict) -> bool:
    """Validate that intent persisted in pipeline state."""
    result = response.get("result", {})

    # Check if intent is in the result (pipeline state)
    if "intent" not in result:
        return False

    intent = result["intent"]
    return isinstance(intent, dict) and "intent" in intent
