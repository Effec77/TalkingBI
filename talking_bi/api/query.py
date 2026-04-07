"""
Query API - Phase 9

Slim HTTP layer that delegates to QueryOrchestrator.
No business logic here - just HTTP boundary concerns.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Any, Dict, Optional

from models.contracts import OrchestratorResult
from services.orchestrator import get_orchestrator
from services.session_manager import get_session, delete_session
from services.query_suggester import generate_suggestions
from services.dataset_awareness import answer_dataset_question, build_dataset_summary
from services.dataset_query_engine import answer_data_question
from auth.dependencies import get_current_user, get_current_user_optional

router = APIRouter()


class QueryPayload(BaseModel):
    """Request body for query endpoint."""

    query: str = ""


def _trace_envelope(trace_data: Any) -> Dict[str, Any]:
    return {
        "available": trace_data is not None,
        "data": trace_data if trace_data is not None else {},
    }


def _build_context_suggestions(
    profile: Dict[str, Any], session: Dict[str, Any], user_query: str, intent: Dict[str, Any] | None
) -> Dict[str, Any]:
    context = {
        "last_query": user_query,
        "kpi": (intent or {}).get("kpi") or (intent or {}).get("kpi_1"),
        "dimension": (intent or {}).get("dimension"),
        "intent": (intent or {}).get("intent"),
    }
    session["suggestion_context"] = context
    return generate_suggestions(profile, context=context)


def _attach_meta_and_trace(
    payload: Dict[str, Any], trace_data: Any, trace_enabled: bool = False
) -> Dict[str, Any]:
    payload["trace"] = _trace_envelope(trace_data)
    payload["meta"] = {"trace_enabled": trace_enabled}
    return payload


def _verify_session_access(session_id: str, user: Optional[Any]) -> Dict[str, Any]:
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404, detail=f"Session {session_id} not found or expired"
        )

    # Public sessions are intentionally accessible without auth token.
    if session.get("user_id") == "public":
        return session

    if user is None:
        raise HTTPException(
            status_code=401, detail="Authentication required for this session"
        )

    # Check ownership
    is_owner = session.get("user_id") == user.id

    # Check org admin access (must belong to same org and be admin)
    is_org_admin = (
        user.role == "admin"
        and user.org_id is not None
        and session.get("org_id") == user.org_id
    )

    if not (is_owner or is_org_admin):
        # Hide existence to prevent session scanning
        raise HTTPException(
            status_code=404, detail=f"Session {session_id} not found or expired"
        )

    return session


@router.post("/query/{session_id}")
async def query_endpoint(session_id: str, payload: QueryPayload, user=Depends(get_current_user_optional)):
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
    user_email = getattr(user, "email", "public")
    print(f"[QUERY] User={user_email}, session={session_id}, query='{user_query}'")

    # Guard 1: Query length
    if len(user_query) > 500:
        raise HTTPException(
            status_code=400, detail="Query too long (max 500 characters)"
        )

    # Guard 2: Session validation
    session = _verify_session_access(session_id, user)

    df = session.get("df")

    app_mode = (session.get("app_mode") or "both").lower()
    if app_mode == "dashboard":
        return _attach_meta_and_trace({
            "status": "MODE_BLOCKED",
            "query": user_query,
            "session_id": session_id,
            "message": "Querying is disabled in dashboard-only mode.",
            "intent": {"intent": "MODE_BLOCKED", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "mode_guard"},
            "data": [],
            "charts": [],
            "primary_insight": "Querying is disabled in dashboard-only mode.",
            "insights": [],
            "candidates": [],
            "plan": {"mode": "dashboard_only"},
            "latency_ms": 0.0,
            "warnings": ["Query mode disabled for this session."],
            "errors": [],
            "suggestions": {"type": "followup", "items": []},
        }, {"parser_used": "mode_guard"})

    # DAL metadata QA path (Phase 11)
    # Deterministic, no LLM, answers dataset-understanding questions directly.
    profile = session.get("dil_profile", {}) or {}
    dataset_summary = session.get("dataset_summary", {}) or {}
    if not dataset_summary or "dimension_values" not in dataset_summary:
        try:
            dataset_summary = build_dataset_summary(df, profile)
            session["dataset_summary"] = dataset_summary
        except Exception:
            dataset_summary = dataset_summary or {}
    dal_answer = answer_dataset_question(user_query, dataset_summary, profile)
    if dal_answer:
        suggestions_payload = _build_context_suggestions(
            profile, session, user_query, {"intent": "DATASET_AWARENESS"}
        )
        return _attach_meta_and_trace({
            "status": "RESOLVED",
            "query": user_query,
            "session_id": session_id,
            "intent": {"intent": "DATASET_AWARENESS", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "dataset_awareness"},
            "data": [],
            "charts": [],
            "primary_insight": dal_answer,
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
            "suggestions": {
                "type": suggestions_payload.get("type", "followup"),
                "items": suggestions_payload.get("items", []),
            },
        }, {"parser_used": "dataset_awareness"})

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
        suggestions_payload = _build_context_suggestions(
            profile, session, user_query, {"intent": "DATASET_QUERY"}
        )
        return _attach_meta_and_trace({
            "status": "RESOLVED",
            "query": user_query,
            "session_id": session_id,
            "intent": {"intent": "DATASET_QUERY", "kpi": None, "dimension": None, "filter": None},
            "semantic_meta": {"applied": False, "source": "dataset_query_engine"},
            "data": [{"kpi": "answer", "type": "timeseries", "data": table}] if table else [],
            "charts": charts,
            "primary_insight": answer_text,
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
            "suggestions": {
                "type": suggestions_payload.get("type", "followup"),
                "items": suggestions_payload.get("items", []),
            },
        }, {"parser_used": "dataset_query_engine"})

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

    response = result.to_dict()
    suggestions_payload = _build_context_suggestions(
        profile, session, user_query, response.get("intent")
    )

    # Derive primary insight from insight list if possible.
    primary_insight = None
    for item in response.get("insights", []) or []:
        if isinstance(item, dict):
            primary_insight = item.get("summary") or item.get("text")
        elif isinstance(item, str):
            primary_insight = item
        if primary_insight:
            break
    response["primary_insight"] = primary_insight
    response["suggestions"] = {
        "type": suggestions_payload.get("type", "followup"),
        "items": suggestions_payload.get("items", []),
    }

    trace_data = response.get("trace")
    print(f"[QUERY] Success: session={session_id}, status={response.get('status')}")
    return _attach_meta_and_trace(response, trace_data, trace_enabled=False)


@router.delete("/session/{session_id}")
async def delete_session_endpoint(session_id: str, user=Depends(get_current_user)):
    """
    Explicitly delete a session and free resources.

    Returns:
        Success confirmation
    """
    session = _verify_session_access(session_id, user)

    success = delete_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    return {
        "session_id": session_id,
        "status": "deleted",
        "message": "Session and all associated data removed",
    }


@router.get("/session/{session_id}/status")
async def get_session_status(session_id: str, user=Depends(get_current_user)):
    """
    Get health check status for a session.

    Returns:
        Session metadata and health indicators
    """
    session = _verify_session_access(session_id, user)

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
async def suggest_queries(session_id: str, q: str = "", user=Depends(get_current_user_optional)):
    """
    Deterministic query suggestions from DIL profile.

    Query params:
    - session_id: required session identifier
    - q: optional prefix filter (e.g. "show rev")
    """
    session = _verify_session_access(session_id, user)

    profile = session.get("dil_profile") or {}
    context = session.get("suggestion_context") or None
    result = generate_suggestions(profile, context=context, prefix=q or "")

    return {
        "session_id": session_id,
        "prefix": q or "",
        "suggestions": {
            "type": result.get("type", "initial"),
            "items": result.get("items", result.get("suggestions", [])),
        },
    }


@router.get("/suggest/{session_id}")
async def suggest_queries_by_path(session_id: str, q: str = "", user=Depends(get_current_user_optional)):
    """Path alias for suggestion endpoint."""
    return await suggest_queries(session_id=session_id, q=q, user=user)
