"""
Phase 9: Query Orchestrator

Control plane for the Talking BI pipeline.
Coordinates all phases (6E → 7 → 6D) without knowing HTTP details.
"""

import time
from typing import Dict, Any, Optional
from dataclasses import asdict
from uuid import uuid4

from models.contracts import OrchestratorResult, ExecutionTrace
from services.session_manager import get_session, SESSION_STORE
from services.conversation_manager import get_conversation_manager
from services.intelligence_engine import generate_dashboard_plan
from services.query_normalizer import create_normalizer
from services.schema_mapper import create_schema_mapper
from services.deterministic_override import DeterministicIntentDetector
from services.intent_parser import parse_intent
from services.intent_validator import validate_intent
from services.semantic_interpreter import create_semantic_interpreter
from services.context_resolver import create_resolver, ResolutionStatus
from services.execution_planner import ExecutionPlanner
from services.evaluator import evaluator_singleton, timed_record
from graph.df_registry import register_df, deregister_df


class QueryOrchestrator:
    """
    Control plane for query processing.

    Responsibilities:
    - Load session and context
    - Run full pipeline (6E → 7 → 6D)
    - Call execution backend
    - Record evaluator metrics
    - Return standardized OrchestratorResult

    Design: Control only, no business logic.
    """

    def __init__(self):
        self.conv_manager = get_conversation_manager()
        self.execution_planner = ExecutionPlanner()

    def handle(self, query: str, session_id: str) -> OrchestratorResult:
        """
        Process a query through the full pipeline.

        11-Step Pipeline:
        1. Load session
        2. Generate dashboard plan
        3. Normalize (6E)
        4. Deterministic override (6G)
        5. Parse (6B) - if needed
        6. Semantic interpret (7)
        7. Schema map (6F)
        8. Validate
        9. Resolve context (6C)
        10. Plan & execute (6D)
        11. Record & return
        """
        start_time = time.time()

        # Initialize trace
        trace = ExecutionTrace()

        try:
            # Step 1: Load session
            upload_session = get_session(session_id)
            if not upload_session:
                return self._error_result(
                    query, session_id, "Session not found or expired", trace, start_time
                )

            df = upload_session["df"]
            metadata = upload_session.get("metadata")
            if not metadata:
                return self._error_result(
                    query, session_id, "Session metadata not found", trace, start_time
                )

            # Get conversation context
            conv_session = self.conv_manager.get_or_create(session_id)
            context_history = conv_session.run_history if conv_session else []

            # Step 2: Generate dashboard plan
            plan = generate_dashboard_plan(
                session_id=session_id,
                df=df,
                uploaded_dataset=metadata,
            )

            dataset_columns = metadata.columns
            kpi_candidates = (
                plan.kpi_candidates if hasattr(plan, "kpi_candidates") else []
            )

            # Step 3: Normalize (6E)
            normalizer = create_normalizer(dataset_columns, kpi_candidates)
            normalized_query, norm_metadata = normalizer.normalize(query)

            trace.normalized_query = normalized_query
            trace.normalization_applied = normalized_query != query
            trace.normalization_changes = norm_metadata.get("modifications", [])

            # Step 4: Deterministic override (6G)
            schema_mapper = create_schema_mapper(df, kpi_candidates)
            detector = DeterministicIntentDetector(schema_mapper, context_history)
            deterministic_intent = detector.detect(normalized_query)

            if deterministic_intent:
                intent = deterministic_intent
                trace.g6_applied = True
                trace.g6_reason = f"deterministic: {intent.get('intent')}"
                trace.parser_used = "deterministic"
            else:
                # Step 5: Parse (6B)
                intent = parse_intent(normalized_query)
                trace.parser_used = "llm"

            trace.raw_intent = intent.copy()

            # Step 6: Semantic interpret (7)
            semantic_interpreter = create_semantic_interpreter(df, schema_mapper)
            intent = semantic_interpreter.interpret(normalized_query, intent)

            if intent.get("semantic_meta", {}).get("applied"):
                trace.semantic_applied = True
                meta = intent["semantic_meta"]
                trace.semantic_mapping = meta.get("mapped_to")
                trace.semantic_confidence = meta.get("confidence", 0.0)

            # Step 7: Schema map (6F)
            intent = schema_mapper.map_intent(intent)

            if intent.get("mapping_meta"):
                meta = intent["mapping_meta"]
                trace.mapped_fields = {
                    "kpi": meta.get("kpi_source", ""),
                    "dimension": meta.get("dimension_source", ""),
                }

            # Step 8: Validate
            is_valid, error_msg = validate_intent(
                intent, dataset_columns, kpi_candidates
            )

            if not is_valid:
                return self._invalid_result(
                    query,
                    session_id,
                    intent,
                    error_msg,
                    trace,
                    start_time,
                    kpi_candidates,
                    dataset_columns,
                )

            # Step 9: Resolve context (6C)
            resolver = create_resolver(
                kpi_candidates=[k.get("name", "") for k in kpi_candidates],
                ambiguity_map={
                    "sales": ["gross_sales", "net_sales"],
                    "profit": ["gross_profit", "net_profit"],
                },
            )

            # Load context into resolver
            if context_history:
                for run in context_history[-3:]:
                    if (
                        run.get("intent")
                        and run.get("run_id")
                        and not run.get("errors")
                    ):
                        resolver.add_to_context(run["intent"])

            dashboard_plan_dict = {"kpis": [k.get("name", "") for k in kpi_candidates]}
            resolution_result = resolver.resolve(intent, dashboard_plan_dict)

            trace.context_used = resolution_result.context_used
            if resolution_result.context_used and resolution_result.intent:
                trace.context_kpi_inherited = resolution_result.intent.get("kpi")

            # Handle non-resolved statuses
            if resolution_result.status != ResolutionStatus.RESOLVED.value:
                return self._unresolved_result(
                    query, session_id, intent, resolution_result, trace, start_time
                )

            # Step 10: Plan & execute (6D)
            resolved_intent = resolution_result.intent
            intent.update(resolved_intent)

            # Get previous execution state for planning
            prev_state = None  # Will be loaded from session in 9A refinement

            exec_plan = self.execution_planner.plan(
                curr_intent=intent,
                prev_state=prev_state,
            )

            trace.execution_path = exec_plan.operations

            # Execute using current pipeline (will be replaced with backend in Task 3)
            from graph.executor import run_pipeline

            run_id = str(uuid4())
            register_df(run_id, df)

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
                "intent": intent,
            }

            result_state = run_pipeline(initial_state)
            deregister_df(run_id)

            # Update conversation
            self.conv_manager.update_session(session_id, result_state, query)

            # Step 11: Build result
            latency_ms = (time.time() - start_time) * 1000

            result = OrchestratorResult(
                status="RESOLVED",
                query=query,
                session_id=session_id,
                intent=resolved_intent,
                semantic_meta=intent.get("semantic_meta", {}),
                data=result_state.get("prepared_data") or [],
                charts=result_state.get("chart_specs") or [],
                insights=result_state.get("insights") or [],
                plan={
                    "mode": exec_plan.mode,
                    "reuse": exec_plan.reuse,
                    "operations": exec_plan.operations,
                    "reason": exec_plan.reason,
                },
                latency_ms=latency_ms,
                warnings=[f"{w.type}: {w.message}" for w in resolution_result.warnings],
                errors=result_state.get("errors", []),
                trace=trace.to_dict(),
            )

            # Record in evaluator
            self._record_evaluator(query, session_id, result, exec_plan.mode)

            return result

        except Exception as e:
            return self._error_result(query, session_id, str(e), trace, start_time)

    def _error_result(
        self,
        query: str,
        session_id: str,
        error: str,
        trace: ExecutionTrace,
        start_time: float,
    ) -> OrchestratorResult:
        """Build error result."""
        latency_ms = (time.time() - start_time) * 1000

        return OrchestratorResult(
            status="ERROR",
            query=query,
            session_id=session_id,
            intent={},
            semantic_meta={},
            data=[],
            charts=[],
            insights=[],
            plan={},
            latency_ms=latency_ms,
            warnings=[],
            errors=[error],
            trace=trace.to_dict(),
        )

    def _invalid_result(
        self,
        query: str,
        session_id: str,
        intent: Dict,
        error: str,
        trace: ExecutionTrace,
        start_time: float,
        kpi_candidates: list,
        dataset_columns: list,
    ) -> OrchestratorResult:
        """Build invalid intent result."""
        latency_ms = (time.time() - start_time) * 1000

        return OrchestratorResult(
            status="INVALID",
            query=query,
            session_id=session_id,
            intent=intent,
            semantic_meta=intent.get("semantic_meta", {}),
            data=[],
            charts=[],
            insights=[],
            plan={},
            latency_ms=latency_ms,
            warnings=[f"Validation failed: {error}"],
            errors=[],
            trace=trace.to_dict(),
        )

    def _unresolved_result(
        self,
        query: str,
        session_id: str,
        intent: Dict,
        resolution_result: Any,
        trace: ExecutionTrace,
        start_time: float,
    ) -> OrchestratorResult:
        """Build unresolved result (UNKNOWN, AMBIGUOUS, INCOMPLETE)."""
        latency_ms = (time.time() - start_time) * 1000

        return OrchestratorResult(
            status=resolution_result.status,
            query=query,
            session_id=session_id,
            intent=resolution_result.intent or intent,
            semantic_meta=intent.get("semantic_meta", {}),
            data=[],
            charts=[],
            insights=[],
            plan={},
            latency_ms=latency_ms,
            warnings=[f"{w.type}: {w.message}" for w in resolution_result.warnings],
            errors=[],
            trace=trace.to_dict(),
        )

    def _record_evaluator(
        self,
        query: str,
        session_id: str,
        result: OrchestratorResult,
        execution_mode: str,
    ):
        """Record query in evaluator."""
        try:
            evaluator_singleton.record(
                query=query,
                dataset=session_id,
                status=result.status,
                intent=result.intent,
                execution_mode=execution_mode,
                semantic_applied=result.semantic_meta.get("applied", False),
                latency_ms=result.latency_ms,
                failure_type=None if result.status == "RESOLVED" else result.status,
            )
        except Exception as e:
            print(f"[Orchestrator] Evaluator recording failed: {e}")


# Singleton instance
_orchestrator = None


def get_orchestrator() -> QueryOrchestrator:
    """Get singleton orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
