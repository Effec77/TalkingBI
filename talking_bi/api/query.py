"""
Query API - Phase 9

Slim HTTP layer that delegates to QueryOrchestrator.
No business logic here - just HTTP boundary concerns.
"""

from uuid import uuid4
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from models.contracts import OrchestratorResult
from services.orchestrator import get_orchestrator
from services.session_manager import get_session, delete_session

router = APIRouter()


class QueryPayload(BaseModel):
    """Request body for query endpoint."""

    query: str = ""


@router.post("/query/{session_id}")
async def query_endpoint(session_id: str, payload: QueryPayload):
    """
    Phase 9: Conversation-aware query endpoint.

    Delegates all business logic to QueryOrchestrator.
    API layer only handles HTTP concerns:
    - Input validation
    - Session existence check
    - Response serialization

    Args:
        session_id: Session identifier from upload
        payload: Query text from user

    Returns:
        OrchestratorResult as JSON
    """
    user_query = payload.query

    # Guard 1: Query length
    if len(user_query) > 500:
        raise HTTPException(
            status_code=400, detail="Query too long (max 500 characters)"
        )

    # Guard 2: Session validation
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404, detail=f"Session {session_id} not found or expired"
        )

    # Guard 3: Dataset size
    df = session.get("df")
    if df is not None and df.shape[0] > 100_000:
        raise HTTPException(
            status_code=413, detail="Dataset too large (max 100,000 rows)"
        )

    # Delegate to orchestrator
    orchestrator = get_orchestrator()
    result = orchestrator.handle(user_query, session_id)

    # Return as JSON
    return result.to_dict()


@router.delete("/session/{session_id}")
async def delete_session_endpoint(session_id: str):
    """
    Explicitly delete a session and free resources.

    Returns:
        Success confirmation
    """
    success = delete_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    return {
        "session_id": session_id,
        "status": "deleted",
        "message": "Session and all associated data removed",
    }


@router.get("/session/{session_id}/status")
async def get_session_status(session_id: str):
    """
    Get health check status for a session.

    Returns:
        Session metadata and health indicators
    """
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404, detail=f"Session {session_id} not found or expired"
        )

    return {
        "session_id": session_id,
        "status": "active",
        "created_at": session.get("created_at"),
        "expires_at": session.get("expires_at"),
        "dataset_shape": session.get("df").shape
        if session.get("df") is not None
        else None,
        "conversation_turns": len(session.get("conversation", [])),
        "evaluation_records": len(session.get("evaluation_records", [])),
    }
