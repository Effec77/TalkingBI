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
from services.evaluator import get_evaluator, timed_record
from graph.df_registry import register_df, deregister_df
from services.cache import query_cache, get_query_key, llm_cache
from services.postgres_backend import PostgresBackend
from services.execution_backend import PandasBackend
from services.query_preprocessor import preprocess_query



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

            # TASK 5: QUERY RESULT CACHE (High Impact)
            from services.query_normalizer import create_normalizer
            cols = metadata.columns if hasattr(metadata, "columns") else metadata.get("columns", [])
            fname = metadata.filename if hasattr(metadata, "filename") else metadata.get("filename", "unknown")
            
            norm_pre = create_normalizer(cols, [])
            norm_q, _ = norm_pre.normalize(query)
            
            # Get conversation context for cache key
            conv_session_cache = self.conv_manager.get_or_create(session_id)
            context_history_cache = conv_session_cache.run_history if conv_session_cache else []
            last_intent_cache = context_history_cache[-1].get("intent") if context_history_cache else None
            
            q_key = get_query_key(norm_q, fname, context=last_intent_cache)
            
            from services.cache import USE_CACHE
            if USE_CACHE and q_key in query_cache:
                print(f"[ORCHESTRATOR] ⚡ Query cache hit!")
                from services.cache import stats
                stats.query_cache_hits += 1
                trace.cache_hit = True
                cached_result = query_cache[q_key]
                # Update trace on cached result before returning
                cached_result.trace["cache_hit"] = True 
                return cached_result
            trace.cache_hit = False

            # Get conversation context
            conv_session = self.conv_manager.get_or_create(session_id)
            context_history = conv_session.run_history if conv_session else []

            # Step 1.5: Fix the dataset_columns access
            dataset_columns = metadata.columns if hasattr(metadata, 'columns') else metadata.get('columns', [])
            
            # Step 2: Generate dashboard plan
            plan = generate_dashboard_plan(
                session_id=session_id,
                df=df,
                uploaded_dataset=metadata,
            )

            kpi_candidates = (
                plan.kpi_candidates if hasattr(plan, "kpi_candidates") else []
            )

            # Step 2.5: Preprocess query (Phase 9C.2)
            # Runs deterministic normalization before any parsing.
            # Uses last resolved intent as context for compare/trend completion.
            last_intent = context_history[-1].get("intent") if context_history else {}
            preprocessed_query = preprocess_query(query, last_intent)
            if preprocessed_query != query:
                print(f"[9C.2:PREPROCESSOR] {repr(query)} -> {repr(preprocessed_query)}")
            trace.preprocessed_query = preprocessed_query

            # Step 3: Normalize (6E)
            normalizer = create_normalizer(dataset_columns, kpi_candidates)
            normalized_query, norm_metadata = normalizer.normalize(preprocessed_query)

            trace.normalized_query = normalized_query
            trace.normalization_applied = normalized_query != query
            trace.normalization_changes = norm_metadata.get("modifications", [])

            # Step 4: Deterministic override (6G)
            schema_mapper = create_schema_mapper(df, kpi_candidates)
            detector = DeterministicIntentDetector(schema_mapper, context_history)

            # FIX 2: Filter Noun Interpretation (Phase 9C.1)
            if normalized_query.lower().startswith("filter"):
                tokens = normalized_query.split()
                if len(tokens) == 2:
                    column_term = tokens[1]
                    # Map to actual column
                    mapped_col, _ = schema_mapper.map_dimension(column_term)
                    if mapped_col:
                        print(f"[ORCHESTRATOR] 🔍 Filter noun detected: {mapped_col}")
                        intent = {
                            "intent": "SUMMARIZE",
                            "kpi": None,
                            "dimension": None,
                            "filter": {
                                "column": mapped_col,
                                "operator": "NOT_NULL"
                            }
                        }
                        trace.filter_interpretation = "NOT_NULL"
                        trace.g6_applied = True
                        trace.g6_reason = "filter_noun"
                        # Execute early
                        deterministic_intent = intent

            # Rule 4: Trend detection
            deterministic_intent = detector.detect(normalized_query)
            # Rule 1/2/3/4/6: Robust Trend Detection
            query_lower = normalized_query.lower()
            force_trend = any(k in query_lower for k in ["trend", "trends", "over time"])
            
            detected_date_col = None
            if force_trend:
                # Priority 1: Profile-based datetime detection
                from services.dataset_profiler import profile_dataset
                profile = profile_dataset(df)
                if profile.datetime_columns:
                    detected_date_col = profile.datetime_columns[0]
                
                # Priority 2: Name-based fallback
                if not detected_date_col:
                    for col in df.columns:
                        if any(k in col.lower() for k in ["date", "time", "month", "year"]):
                            detected_date_col = col
                            break
                
                if detected_date_col:
                    print(f"[ORCHESTRATOR] Trend detected, using dimension: {detected_date_col}")
                    trace.trend_detected = True
                    trace.trend_dimension = detected_date_col
                    trace.trend_locked = True
                    
                    # Rule 2: Force Intent & LOCK (Phase 9C.1)
                    trend_intent = {
                        "intent": "SEGMENT_BY",
                        "dimension": detected_date_col,
                        "kpi": None,
                        "filter": None,
                        "_locked": True,
                        "_lock_source": "trend"
                    }
                    
                    # Ensure deterministic_intent reflects this
                    if deterministic_intent:
                        deterministic_intent.update(trend_intent)
                    else:
                        deterministic_intent = trend_intent
                else:
                    # Rule 4: Fail Safe
                    print("[ORCHESTRATOR] Trend requested but no date column found")
                    return OrchestratorResult(
                        status="INCOMPLETE",
                        query=query,
                        session_id=session_id,
                        intent={"intent": "SEGMENT_BY", "kpi": None, "dimension": None},
                        semantic_meta={},
                        data=[],
                        charts=[],
                        warnings=["No time dimension found for trend analysis"],
                        trace=trace.to_dict(),
                        latency_ms=(time.time() - start_time) * 1000
                    )
            # End Trend Logic

            if deterministic_intent:
                intent = deterministic_intent
                trace.g6_applied = True
                trace.g6_reason = f"deterministic: {intent.get('intent')}"
                trace.parser_used = "deterministic"
            else:
                # Step 5: Parse (6B)
                intent = parse_intent(normalized_query)
                trace.parser_used = "llm"

                # FIX 5: Handle LLM null response
                if intent is None:
                    print("[ORCHESTRATOR] LLM returned None, using empty intent")
                    intent = {
                        "intent": "UNKNOWN",
                        "kpi": None,
                        "dimension": None,
                        "filter": None,
                    }

            trace.raw_intent = intent.copy() if intent else {}

            # Step 6: Semantic interpret (7)
            semantic_interpreter = create_semantic_interpreter(df, schema_mapper)
            print(f"[TRACE:ORCHESTRATOR] BEFORE SEMANTIC: {intent}")
            intent = semantic_interpreter.interpret(preprocessed_query, intent)
            print(f"[TRACE:ORCHESTRATOR] AFTER SEMANTIC: {intent}")

            if intent.get("semantic_meta", {}).get("applied"):
                trace.semantic_applied = True
                meta = intent["semantic_meta"]
                trace.semantic_mapping = meta.get("mapped_to")
                trace.semantic_confidence = meta.get("confidence", 0.0)

                        # Task 8: Confidence Scoring (Phase 9C)
                        # Task 8: Confidence Scoring (Phase 9C)
            kpi_term = intent.get("kpi")
            dim_term = intent.get("dimension")
            
            kpi_conf = 1.0 if kpi_term in df.columns else 0.5 if kpi_term else 0.0
            sem_conf = trace.semantic_confidence or 0.0
            ctx_conf = 1.0 if trace.context_used else 0.0
            
            # For COMPARE intents, if kpi_1 or kpi_2 is present, we have some confidence
            if intent.get("intent") == "COMPARE":
                if intent.get("kpi_1") or intent.get("kpi_2") or intent.get("filter"):
                    kpi_conf = max(kpi_conf, 0.5)
            
            overall_conf = max(kpi_conf, sem_conf, ctx_conf)
            if not kpi_term and not dim_term and intent.get("intent") != "COMPARE":
                overall_conf = 0.0
                
            trace.confidence = {
                "kpi": kpi_conf,
                "semantic": sem_conf,
                "context": ctx_conf,
                "overall": overall_conf
            }
            
            # Rule 8: If overall confidence is low, mark as INCOMPLETE/UNKNOWN
            # Phase 9C.1 Bypass for locked or special intents.
            # Bypass COMPARE as it resolves its confidence in Phase 6C later.
            if overall_conf < 0.5 and intent.get("intent") not in ["UNKNOWN", "COMPARE"] and not intent.get("_locked") and not trace.filter_interpretation:
                print(f"[ORCHESTRATOR] Low confidence query ({overall_conf}), marking INCOMPLETE")
                trace.failure_reason = {
                    "type": "SEMANTIC_FAIL",
                    "stage": "7",
                    "details": "Low confidence in intent resolution"
                }
                return self._unresolved_result(
                    query, session_id, intent, 
                    type('Res', (object,), {"status": "INCOMPLETE", "intent": intent, "warnings": [type('Warn', (object,), {"type": "confidence", "message": "Low confidence query"})], "context_used": False, "context_applied": False})(),
                    trace, start_time
                )

            # Step 7: Schema map (6F)
            print(f"[TRACE:ORCHESTRATOR] BEFORE SCHEMA: {intent}")
            intent = schema_mapper.map_intent(intent)
            print(f"[TRACE:ORCHESTRATOR] AFTER SCHEMA: {intent}")
            
            # FIX 1: KPI Validation debug trace
            trace.kpi_validation = {
                "input": intent.get("kpi"),
                "matched_column": intent.get("kpi") if intent.get("kpi") in df.columns else None,
                "columns_available": list(df.columns)
            }

            if intent.get("mapping_meta"):
                meta = intent["mapping_meta"]
                
                # Check for explicit ambiguity from Schema Mapper
                ambiguous_candidates = meta.get("ambiguous_candidates", {})
                if ambiguous_candidates:
                    latency_ms = (time.time() - start_time) * 1000
                    all_candidates = []
                    for k, v in ambiguous_candidates.items():
                        if isinstance(v, list): all_candidates.extend(v)
                    all_candidates = list(set(all_candidates)) # deduplicate
                    
                    return OrchestratorResult(
                        status="AMBIGUOUS",
                        query=query,
                        session_id=session_id,
                        intent=intent,
                        semantic_meta=intent.get("semantic_meta", {}),
                        data=[],
                        charts=[],
                        insights=[],
                        candidates=all_candidates,
                        plan={},
                        latency_ms=latency_ms,
                        warnings=[f"Ambiguous mapping detected for one or more fields. Candidates: {all_candidates}"],
                        errors=[],
                        trace=trace.to_dict(),
                    )
                    
                # 9C.2: If 6G explicitly tagged kpi_source (e.g. "context"),
                # preserve it — don't let schema mapper overwrite provenance.
                kpi_source_override = trace.raw_intent.get("kpi_source") if hasattr(trace, "raw_intent") and isinstance(trace.raw_intent, dict) else None
                trace.mapped_fields = {
                    "kpi": kpi_source_override or meta.get("kpi_source", ""),
                    "dimension": meta.get("dimension_source", ""),
                }

            # (Removed early partial intent check here, see after resolution)

            # FIX 1: Enhanced validation - check against actual columns too
            # First try normal validation with KPI candidates
            is_valid, error_msg = validate_intent(
                intent, dataset_columns, kpi_candidates
            )

            # If validation failed due to invalid KPI, check if it exists in actual columns
            if not is_valid and error_msg == "invalid_kpi":
                # Check if the KPI exists as a column in the dataset
                kpi_name = intent.get("kpi", "")
                kpi_normalized = kpi_name.lower().strip() if kpi_name else ""
                column_lower = {col.lower(): col for col in dataset_columns}

                if kpi_normalized in column_lower:
                    # KPI exists as a column exactly, accept it
                    print(
                        f"[ORCHESTRATOR] KPI '{kpi_name}' found exactly in dataset columns, accepting"
                    )
                    intent["kpi"] = column_lower[kpi_normalized]
                    is_valid = True
                    error_msg = None
                else:
                    # Check substring match (e.g. 'total amount' in 'amount' or vice versa)
                    for col_norm, col_raw in column_lower.items():
                        if kpi_normalized in col_norm or col_norm in kpi_normalized:
                            print(
                                f"[ORCHESTRATOR] KPI '{kpi_name}' matches column '{col_raw}', accepting"
                            )
                            intent["kpi"] = col_raw
                            is_valid = True
                            error_msg = None
                            break

                        # Rule 1/6 Trace
            trace.kpi_resolution = {
                "input": intent.get("kpi"),
                "resolved_to": intent.get("kpi") if is_valid else None,
                "columns": list(df.columns)
            }
            
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
            print(f"[TRACE:ORCHESTRATOR] BEFORE RESOLVER: {intent}")
            resolution_result = resolver.resolve(intent, dashboard_plan_dict, current_columns=list(df.columns))
            print(f"[TRACE:ORCHESTRATOR] AFTER RESOLVER: {resolution_result.intent if resolution_result else 'None'}")

            trace.context_used = resolution_result.context_used
            trace.context_applied = getattr(resolution_result, "context_applied", False)
            if resolution_result.context_used and resolution_result.intent and hasattr(resolution_result.intent, "get"):
                trace.context_kpi_inherited = resolution_result.intent.get("kpi")

            # Handle non-resolved statuses
            if resolution_result.status != ResolutionStatus.RESOLVED.value:
                return self._unresolved_result(
                    query, session_id, intent, resolution_result, trace, start_time
                )

            # Step 10: Plan & execute (6D)
            resolved_intent = resolution_result.intent

            # FIX 3: Context is resolved now. Check for partial query
            if (
                not resolved_intent.get("kpi")
                and not resolved_intent.get("dimension")
                and not resolved_intent.get("filter")
            ):
                latency_ms = (time.time() - start_time) * 1000
                return OrchestratorResult(
                    status="UNKNOWN",
                    query=query,
                    session_id=session_id,
                    intent=resolved_intent,
                    semantic_meta=intent.get("semantic_meta", {}),
                    data=[],
                    charts=[],
                    insights=[],
                    plan={},
                    latency_ms=latency_ms,
                    warnings=["Query could not be understood"],
                    errors=[],
                    trace=trace.to_dict(),
                )

            if not resolved_intent.get("kpi") and (
                resolved_intent.get("dimension") or resolved_intent.get("filter")
            ):
                latency_ms = (time.time() - start_time) * 1000
                return OrchestratorResult(
                    status="INCOMPLETE",
                    query=query,
                    session_id=session_id,
                    intent=resolved_intent,
                    semantic_meta=intent.get("semantic_meta", {}),
                    data=[],
                    charts=[],
                    insights=[],
                    plan={},
                    latency_ms=latency_ms,
                    warnings=["Missing KPI - please specify what metric to analyze"],
                    errors=[],
                    trace=trace.to_dict(),
                )

            # FIX 6: Trend intent minimal support
            # If query mentions trend but intent doesn't have dimension, add time dimension
            if resolved_intent:
                query_lower = query.lower()
                is_trend = any(kw in query_lower for kw in ["trend", "trends", "over time"])
                if is_trend:
                    if not resolved_intent.get("dimension"):
                        # Find datetime column
                        datetime_cols = [
                            col
                            for col in dataset_columns
                            if any(
                                dt in col.lower() for dt in ["date", "time", "month", "year"]
                            )
                        ]
                        if datetime_cols:
                            resolved_intent["dimension"] = datetime_cols[0]
                            print(
                                f"[ORCHESTRATOR] Trend intent detected, added dimension: {datetime_cols[0]}"
                            )
                    
                    # Force intent to SEGMENT_BY so UI renders line chart 
                    resolved_intent["intent"] = "SEGMENT_BY"

            intent.update(resolved_intent)

            # Get previous execution state for planning
            prev_state = None  # Will be loaded from session in 9A refinement

            exec_plan = self.execution_planner.plan(
                curr_intent=intent,
                prev_state=prev_state,
            )

            trace.execution_path = exec_plan.operations

            # FIX 4: Wrap execution in try-except with safe fallback
            try:
                # TASK 3: BACKEND SWITCHING
                dashboard_plan_dict = {
                    **asdict(plan),
                    "_meta": {
                        "kpi_count": len(plan.kpis),
                        "chart_count": len(plan.charts),
                    },
                }
                # Execute using correct backend abstraction
                # Task 6: Backend Consistency (Phase 9C)
                DEBUG_BACKEND_CHECK = False
                USE_POSTGRES = False
                
                if (USE_POSTGRES or DEBUG_BACKEND_CHECK) and upload_session.get("use_postgres"):
                     backend_pd = PandasBackend()
                     res_pd = backend_pd.execute(
                         plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                         df=df, prev_state=prev_state, session_id=session_id
                     )
                     
                     backend_pg = PostgresBackend()
                     res_pg = backend_pg.execute(
                         plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                         df=df, prev_state=prev_state, session_id=session_id
                     )
                     
                     # Simple check
                     pd_out = str(res_pd.final_output)
                     pg_out = str(res_pg.final_output)
                     
                     if pd_out == pg_out or not DEBUG_BACKEND_CHECK:
                         adaptive_res = res_pg if USE_POSTGRES else res_pd
                         backend = backend_pg if USE_POSTGRES else backend_pd
                     else:
                         print("[ORCHESTRATOR] ⚠ Backend inconsistency detected, falling back to Pandas")
                         adaptive_res = res_pd
                         backend = backend_pd
                         trace.fallback_triggered = True
                else:
                    backend = PandasBackend()
                    adaptive_res = backend.execute(
                        plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                        df=df, prev_state=prev_state, session_id=session_id
                    )
                trace.backend_used = backend.name
                
                # Check if it fell back or executed purely in postgres
                if hasattr(adaptive_res, "mode_used") and adaptive_res.mode_used == "postgres":
                    trace.backend_used = "postgres"
                
                result_state = adaptive_res.pipeline_result

            except Exception as exec_error:
                # Execution failed - return UNKNOWN instead of ERROR
                print(f"[ORCHESTRATOR] Execution failed: {exec_error}")
                deregister_df(run_id)  # Clean up
                latency_ms = (time.time() - start_time) * 1000
                return OrchestratorResult(
                    status="UNKNOWN",
                    query=query,
                    session_id=session_id,
                    intent=intent,
                    semantic_meta=intent.get("semantic_meta", {}),
                    data=[],
                    charts=[],
                    insights=[],
                    plan={},
                    latency_ms=latency_ms,
                    warnings=[f"Could not execute query: {str(exec_error)}"],
                    errors=[],
                    trace=trace.to_dict(),
                )

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
            self._record_evaluator(query, session_id, result)

            return result

        except Exception as e:
            # FIX 4: Never return ERROR for system exceptions - use UNKNOWN
            print(f"[ORCHESTRATOR] System exception: {e}")
            import traceback

            traceback.print_exc()
            return self._unknown_result(query, session_id, str(e), trace, start_time)

    def _unknown_result(
        self,
        query: str,
        session_id: str,
        error: str,
        trace: ExecutionTrace,
        start_time: float,
    ) -> OrchestratorResult:
        """Build unknown result - for system exceptions (FIX 4)."""
        latency_ms = (time.time() - start_time) * 1000

        return OrchestratorResult(
            status="UNKNOWN",
            query=query,
            session_id=session_id,
            intent={},
            semantic_meta={},
            data=[],
            charts=[],
            insights=[],
            plan={},
            latency_ms=latency_ms,
            warnings=[f"System could not process query: {error}"],
            errors=[],
            trace=trace.to_dict(),
        )

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
            status="AMBIGUOUS" if error == "ambiguous" else "UNKNOWN",
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
        # Update conversation context for multi-turn support
        result_state = {
            "intent": intent,
            "run_id": str(uuid4()),
            "status": resolution_result.status,
            "warnings": [f"{w.type}: {w.message}" for w in resolution_result.warnings],
        }
        self.conv_manager.update_session(session_id, result_state, query)
        
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
        self, query: str, session_id: str, result: OrchestratorResult
    ):
        """Record query in evaluator."""
        try:
            evaluator = get_evaluator()
            # Convert OrchestratorResult to dict for evaluator
            result_dict = result.to_dict()
            evaluator.record(
                query=query,
                dataset=session_id,
                result=result_dict,
                latency_ms=result.latency_ms,
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
