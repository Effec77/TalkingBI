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
from services.query_suggester import generate_suggestions
from services.dataset_awareness import answer_dataset_question
from services.dataset_query_engine import answer_data_question

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

    app_mode = (session.get("app_mode") or "both").lower()
    if app_mode == "dashboard":
        return {
            "status": "INCOMPLETE",
            "query": user_query,
            "session_id": session_id,
            "intent": {"intent": "MODE_BLOCKED", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "mode_guard"},
            "data": [],
            "charts": [],
            "insights": [
                {
                    "type": "MODE",
                    "summary": "This session is in dashboard mode. Re-upload with mode=query or mode=both to run chat queries.",
                    "text": "This session is in dashboard mode. Re-upload with mode=query or mode=both to run chat queries.",
                }
            ],
            "candidates": [],
            "plan": {"mode": "dashboard_only"},
            "latency_ms": 0.0,
            "warnings": ["Query mode disabled for this session."],
            "errors": [],
            "trace": {"parser_used": "mode_guard"},
        }

    # DAL metadata QA path (Phase 11)
    # Deterministic, no LLM, answers dataset-understanding questions directly.
    profile = session.get("dil_profile", {}) or {}
    dataset_summary = session.get("dataset_summary", {}) or {}
    dal_answer = answer_dataset_question(user_query, dataset_summary, profile)
    if dal_answer:
        return {
            "status": "RESOLVED",
            "query": user_query,
            "session_id": session_id,
            "intent": {"intent": "DATASET_AWARENESS", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "dataset_awareness"},
            "data": [],
            "charts": [],
            "insights": [
                {
                    "type": "DATASET_AWARENESS",
                    "summary": dal_answer,
                    "text": dal_answer,
                }
            ],
            "candidates": [],
            "plan": {"mode": "dataset_awareness"},
            "latency_ms": 0.0,
            "warnings": [],
            "errors": [],
            "trace": {"parser_used": "dataset_awareness"},
        }

    # Dataset Query Engine (Phase 11): deterministic SQL-like QA
    dq_context = session.get("dataset_query_context", {}) or {}
    data_answer = answer_data_question(user_query, df, profile, context=dq_context)
    if data_answer:
        answer_text = data_answer.get("answer", "")
        table = data_answer.get("table", []) or []
        charts = data_answer.get("charts", []) or []
        new_ctx = data_answer.get("context")
        if new_ctx is not None:
            session["dataset_query_context"] = new_ctx
        return {
            "status": "RESOLVED",
            "query": user_query,
            "session_id": session_id,
            "intent": {"intent": "DATASET_QUERY", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "dataset_query_engine"},
            "data": [{"kpi": "answer", "type": "timeseries", "data": table}] if table else [],
            "charts": charts,
            "insights": [
                {
                    "type": "DATASET_QUERY",
                    "summary": answer_text,
                    "text": answer_text,
                }
            ],
            "candidates": [],
            "plan": {"mode": "dataset_query_engine"},
            "latency_ms": 0.0,
            "warnings": [],
            "errors": [],
            "trace": {"parser_used": "dataset_query_engine"},
        }

    # Delegate to orchestrator
    orchestrator = get_orchestrator()
    df = session.get("df")
    
    # Preprocess Phase 9C.3
    if profile and df is not None:
        from services.preprocessor_v2 import preprocess_v2
        user_query = preprocess_v2(user_query, df, profile)
        
    result = orchestrator.handle(user_query, session_id)
    
    # Post-process Phase 9C.3 Clarifications
    if result.status == "INCOMPLETE" and profile:
        from services.clarification_engine import generate_clarifications
        # Determine missing components from context
        missing = []
        if result.intent:
            if not result.intent.get("kpi"):
                missing.append("kpi")
            if not result.intent.get("dimension") and result.intent.get("intent", "UNKNOWN") in ["SEGMENT_BY", "TREND"]:
                missing.append("dimension")
                
        if missing:
            suggestions = generate_clarifications(user_query, profile, missing)
            result.insights.append({
                "type": "SUGGESTION",
                "summary": "Try asking:",
                "text": " • " + "\n • ".join(suggestions)
            })

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


@router.get("/suggest")
async def suggest_queries(session_id: str, q: str = ""):
    """
    Deterministic query suggestions from DIL profile.

    Query params:
    - session_id: required session identifier
    - q: optional prefix filter (e.g. "show rev")
    """
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404, detail=f"Session {session_id} not found or expired"
        )

    profile = session.get("dil_profile") or {}
    app_mode = (session.get("app_mode") or "both").lower()
    if app_mode == "query":
        return {
            "session_id": session_id,
            "prefix": q or "",
            "suggestions": [],
        }

    result = generate_suggestions(profile, prefix=q or "")

    return {
        "session_id": session_id,
        "prefix": q or "",
        "suggestions": result.get("suggestions", []),
    }


@router.get("/suggest/{session_id}")
async def suggest_queries_by_path(session_id: str, q: str = ""):
    """Path alias for suggestion endpoint."""
    return await suggest_queries(session_id=session_id, q=q)
