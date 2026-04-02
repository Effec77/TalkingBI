"""
Query API - Phase 6B

Conversation-based query endpoint with intent parsing.
Stateful interaction with controlled natural language understanding.
"""

from uuid import uuid4
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from graph.df_registry import deregister_df, register_df
from graph.executor import run_pipeline
from services.intelligence_engine import generate_dashboard_plan
from services.session_manager import get_session
from services.conversation_manager import get_conversation_manager
from services.intent_parser import parse_intent
from services.intent_validator import validate_intent, get_clarification_message

router = APIRouter()


class QueryPayload(BaseModel):
    """Request body for query endpoint."""

    query: str = ""


def _serialize_query_results(query_results: list) -> list:
    """
    Strip pandas DataFrames from query_results before HTTP serialization.
    """
    import pandas as pd

    clean = []
    for qr in query_results:
        entry = {k: v for k, v in qr.items() if k != "data"}
        data = qr.get("data")
        if isinstance(data, pd.DataFrame):
            entry["data_shape"] = list(data.shape)
            entry["data_preview"] = data.head(3).to_dict(orient="records")
        elif data is not None:
            entry["data"] = data
        clean.append(entry)
    return clean


@router.post("/query/{session_id}")
async def query_endpoint(session_id: str, payload: QueryPayload):
    """
    Phase 6A: Conversation-aware query endpoint.

    Maintains session state across multiple queries.
    Currently runs full pipeline (partial execution in Phase 6B).

    Args:
        session_id: Session identifier from upload
        payload: Query text from user

    Returns:
        Full pipeline result with session history metadata
    """
    user_query = payload.query

    # Get or create conversation session
    conv_manager = get_conversation_manager()
    session = conv_manager.get_or_create(session_id)

    # Get upload session
    upload_session = get_session(session_id)
    if not upload_session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} not found or expired",
        )

    df = upload_session["df"]
    metadata = upload_session.get("metadata")
    if metadata is None:
        raise HTTPException(status_code=400, detail="Session metadata not found")

    # PHASE 6B: Parse and validate intent
    # LLM acts ONLY as parser - returns structured JSON, never executes

    # PHASE 6E: Generate dashboard plan FIRST to get columns and KPIs for normalization
    try:
        plan = generate_dashboard_plan(
            session_id=session_id,
            df=df,
            uploaded_dataset=metadata,
        )

        # PHASE 6E: Normalize query before parsing
        from services.query_normalizer import create_normalizer
        from services.schema_mapper import create_schema_mapper
        from services.deterministic_override import apply_deterministic_override

        dataset_columns = metadata.columns
        kpi_candidates = plan.kpi_candidates if hasattr(plan, "kpi_candidates") else []

        normalizer = create_normalizer(dataset_columns, kpi_candidates)
        normalized_query, norm_metadata = normalizer.normalize(user_query)

        if normalized_query != user_query:
            print(f"[API:query:6E] Normalized: '{user_query}' -> '{normalized_query}'")
            print(
                f"[API:query:6E] Modifications: {norm_metadata.get('modifications', [])}"
            )

        # PHASE 6G: Deterministic Intent Override (before LLM)
        # Create schema mapper for validation
        from services.deterministic_override import DeterministicIntentDetector

        schema_mapper = create_schema_mapper(df, kpi_candidates)

        # Get conversation context for 6G
        conv_session = conv_manager.get_session(session_id)
        context_history = conv_session.run_history if conv_session else []

        # Try deterministic pattern matching with context
        detector = DeterministicIntentDetector(schema_mapper, context_history)
        deterministic_intent = detector.detect(normalized_query)

        if deterministic_intent:
            # 6G matched - skip LLM
            intent = deterministic_intent
            print(f"[API:query:6G] Using deterministic intent: {intent['intent']}")
        else:
            # No 6G match - use LLM parser
            intent = parse_intent(normalized_query)
            print(f"[API:query:6B] LLM parsed intent: {intent['intent']}")

    except Exception as e:
        print(f"[API:query] Error in planning/normalization: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Query processing failed: {str(e)}"
        )

    try:
        # PHASE 6F: Schema Intelligence Layer
        # Map user terminology to actual dataset schema elements
        from services.schema_mapper import create_schema_mapper

        schema_mapper = create_schema_mapper(df, kpi_candidates)
        intent = schema_mapper.map_intent(intent)

        # Log mapping results
        if intent.get("mapping_meta"):
            meta = intent["mapping_meta"]
            if meta.get("kpi_source"):
                print(f"[API:query:6F] KPI mapped via: {meta['kpi_source']}")
            if meta.get("dimension_source"):
                print(
                    f"[API:query:6F] Dimension mapped via: {meta['dimension_source']}"
                )
            print(
                f"[API:query:6F] Mapping confidence: {meta.get('confidence', 'unknown')}"
            )

        # PHASE 6B: Validate intent against dataset
        # Ensures KPI/column references are real, not hallucinated
        # ✅ CRITICAL FIX: Validate against ALL KPI candidates (not just selected)
        dataset_columns = metadata.columns
        kpi_candidates = plan.kpi_candidates if hasattr(plan, "kpi_candidates") else []

        is_valid, error_msg = validate_intent(intent, dataset_columns, kpi_candidates)

        if not is_valid:
            # FIX 4: Structured error response (UI-agnostic)
            # Return machine-readable error for frontend to handle
            return {
                "session_id": session_id,
                "run_id": None,
                "query": user_query,
                "intent": intent,
                "status": "INVALID",
                "reason": error_msg,
                "candidates": {
                    "kpis": [k.get("name", "") for k in kpi_candidates],
                    "dimensions": dataset_columns,
                },
            }

        print(f"[API:query] Intent validated: {intent['intent']}")

        # PHASE 6C: Context Resolution
        # Resolve incomplete intents using conversation context
        from services.context_resolver import create_resolver, ResolutionStatus

        resolver = create_resolver(
            kpi_candidates=[k.get("name", "") for k in kpi_candidates],
            ambiguity_map={
                "sales": ["gross_sales", "net_sales"],
                "profit": ["gross_profit", "net_profit"],
            },
        )

        # Get conversation session for context
        conv_session = conv_manager.get_session(session_id)
        if conv_session and conv_session.run_history:
            # Replay last resolved intents to resolver context
            for run in conv_session.run_history[-3:]:  # Max 3 turns
                if run.get("intent") and run.get("run_id") and not run.get("errors"):
                    resolver.add_to_context(run["intent"])

        # Resolve the intent
        dashboard_plan_dict = {"kpis": [k.get("name", "") for k in kpi_candidates]}
        resolution_result = resolver.resolve(intent, dashboard_plan_dict)

        print(f"[API:query] Resolution: {resolution_result.status}")

        # Handle resolution outcomes
        if resolution_result.status == ResolutionStatus.UNKNOWN.value:
            return {
                "session_id": session_id,
                "run_id": None,
                "query": user_query,
                "intent_raw": intent,
                "intent_resolved": None,
                "status": "UNKNOWN",
                "reason": "Could not understand query",
                "source_map": {},
                "warnings": [],
                "charts_generated": 0,
                "insights_generated": 0,
                "execution_trace": [],
            }

        if resolution_result.status == ResolutionStatus.AMBIGUOUS.value:
            return {
                "session_id": session_id,
                "run_id": None,
                "query": user_query,
                "intent_raw": intent,
                "intent_resolved": None,
                "status": "AMBIGUOUS",
                "ambiguity": resolution_result.ambiguity,
                "source_map": {},
                "warnings": [],
                "charts_generated": 0,
                "insights_generated": 0,
                "execution_trace": [],
            }

        if resolution_result.status == ResolutionStatus.INCOMPLETE.value:
            return {
                "session_id": session_id,
                "run_id": None,
                "query": user_query,
                "intent_raw": intent,
                "intent_resolved": resolution_result.intent,
                "status": "INCOMPLETE",
                "missing_fields": resolution_result.missing_fields,
                "source_map": resolution_result.source_map,
                "warnings": [
                    {"type": w.type, "field": w.field, "message": w.message}
                    for w in resolution_result.warnings
                ],
                "charts_generated": 0,
                "insights_generated": 0,
                "execution_trace": [],
            }

        # RESOLVED - proceed with execution
        resolved_intent = resolution_result.intent

        # Update intent with resolved values for pipeline
        intent.update(resolved_intent)

        # Register DataFrame for this run
        run_id = str(uuid4())
        register_df(run_id, df)

        # Build initial state (same structure as /run)
        # FIX 1: Persist intent in pipeline state for Phase 6C context resolution
        from dataclasses import asdict

        initial_state = {
            "session_id": session_id,
            "dataset": {
                "filename": metadata.filename,
                "columns": metadata.columns,
                "shape": metadata.shape,
            },
            "dashboard_plan": {
                **asdict(plan),
                "_meta": {
                    "kpi_count": len(plan.kpis),
                    "chart_count": len(plan.charts),
                },
            },
            "shared_context": {},
            "query_results": [],
            "prepared_data": None,
            "insights": [],
            "chart_specs": [],
            "insight_summary": None,
            "transformed_data": None,
            "retry_flags": {},
            "execution_trace": [],
            "is_refinement": False,
            "target_components": [],
            "retry_count": 0,
            "errors": [],
            "run_id": run_id,
            "parent_run_id": None,
            "intent": intent,  # FIX 1: Persist parsed intent in state
        }

        # Execute pipeline (full run for now)
        result_state = run_pipeline(initial_state)

        # FIX 5: Defensive state validation
        assert "run_id" in result_state, "Pipeline result missing run_id"
        assert "chart_specs" in result_state, "Pipeline result missing chart_specs"
        assert "insights" in result_state, "Pipeline result missing insights"
        assert "session_id" in result_state, "Pipeline result missing session_id"

        # Cleanup DataFrame registry
        deregister_df(run_id)

        # Update conversation session
        conv_manager.update_session(session_id, result_state, user_query)

        # Build response with Phase 6B/6C info
        response = {
            "session_id": result_state["session_id"],
            "run_id": result_state["run_id"],
            "query": user_query,
            "intent_raw": intent,
            "intent_resolved": resolved_intent,
            "status": "RESOLVED",
            "source_map": resolution_result.source_map,
            "warnings": [
                {
                    "type": w.type,
                    "field": w.field,
                    "value": w.value,
                    "message": w.message,
                }
                for w in resolution_result.warnings
            ],
            "context_used": resolution_result.context_used,
            "errors": result_state.get("errors", []),
            "query_results": _serialize_query_results(
                result_state.get("query_results", [])
            ),
            "prepared_data": result_state.get("prepared_data") or [],
            "transformed_data": result_state.get("transformed_data") or [],
            "insights": result_state.get("insights") or [],
            "insight_summary": result_state.get("insight_summary"),
            "chart_specs": result_state.get("chart_specs") or [],
            "execution_trace": result_state.get("execution_trace", []),
            "charts_generated": len(result_state.get("chart_specs") or []),
            "insights_generated": len(result_state.get("insights") or []),
            "_summary": {
                "kpis_executed": len(result_state.get("query_results", [])),
                "kpis_succeeded": sum(
                    1
                    for r in result_state.get("query_results", [])
                    if r.get("status") in ("success", "retry_success")
                ),
                "kpis_retried": sum(
                    1
                    for r in result_state.get("query_results", [])
                    if r.get("status") == "retry_success"
                ),
                "charts": len(result_state.get("chart_specs") or []),
                "insights": len(result_state.get("insights") or []),
            },
            # Phase 6A additions
            "conversation": {
                "turn_count": len(session.conversation_turns),
                "run_count": len(session.run_history),
                "history_length": len(session.run_history),
            },
        }

        # Add UI block (same as /run)
        planned_kpis = {
            k.get("name", "")
            for k in result_state.get("dashboard_plan", {}).get("kpis", [])
        }

        top_kpis = result_state.get("transformed_data") or []
        top_kpis = [
            k
            for k in top_kpis
            if (abs(k.get("max", 0) - k.get("min", 0)) * (k.get("points", 1) ** 0.5))
            >= 10
        ]
        top_kpis = [k for k in top_kpis if k.get("kpi") in planned_kpis]

        top_insights = (result_state.get("insights") or [])[:3]
        ui_charts = result_state.get("chart_specs") or []
        ui_charts = [c for c in ui_charts if c.get("kpi") in planned_kpis]

        response["ui"] = {
            "summary": result_state.get("insight_summary"),
            "top_kpis": top_kpis,
            "top_insights": top_insights,
            "charts": ui_charts,
        }

        print(
            f"[API:query] Session {session_id}: turn {len(session.conversation_turns)}"
        )
        return response

    except Exception as e:
        print(f"[API:query] Error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


@router.get("/query/{session_id}/history")
async def get_conversation_history(session_id: str):
    """
    Get conversation history for a session.

    Returns list of all queries and their associated run metadata.
    """
    conv_manager = get_conversation_manager()
    session = conv_manager.get_session(session_id)

    if not session:
        raise HTTPException(
            status_code=404, detail=f"Conversation session {session_id} not found"
        )

    return {
        "session_id": session_id,
        "turns": [
            {
                "turn": i + 1,
                "query": turn.get("query", ""),
                "run_id": turn.get("run_id"),
            }
            for i, turn in enumerate(session.conversation_turns)
        ],
        "stats": session.get_stats(),
    }
