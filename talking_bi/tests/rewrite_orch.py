import codecs

filepath = 'services/orchestrator.py'
content = codecs.open(filepath, 'r', 'utf-8').read()

# Add imports
imports_chunk = '''from services.context_resolver import create_resolver, ResolutionStatus
from services.execution_planner import ExecutionPlanner
from services.evaluator import get_evaluator, timed_record
from graph.df_registry import register_df, deregister_df
from services.cache import query_cache, get_query_key, llm_cache
from services.postgres_backend import PostgresBackend
from services.execution_backend import PandasBackend
'''

if 'from services.cache' not in content:
    content = content.replace(
        'from services.context_resolver import create_resolver, ResolutionStatus\nfrom services.execution_planner import ExecutionPlanner\nfrom services.evaluator import get_evaluator, timed_record\nfrom graph.df_registry import register_df, deregister_df',
        imports_chunk
    )

# Add early query cache checking
early_cache_check = '''            df = upload_session["df"]
            metadata = upload_session.get("metadata")
            if not metadata:
                return self._error_result(
                    query, session_id, "Session metadata not found", trace, start_time
                )

            # TASK 5: QUERY RESULT CACHE (High Impact)
            q_key = get_query_key(query, metadata.filename)
            if q_key in query_cache:
                print(f"[ORCHESTRATOR] ⚡ Query cache hit!")
                trace.cache_hit = True
                cached_result = query_cache[q_key]
                # Update trace on cached result before returning
                cached_result.trace["cache_hit"] = True 
                return cached_result
            trace.cache_hit = False

            # Get conversation context'''

content = content.replace(
    '''            df = upload_session["df"]
            metadata = upload_session.get("metadata")
            if not metadata:
                return self._error_result(
                    query, session_id, "Session metadata not found", trace, start_time
                )

            # Get conversation context''',
    early_cache_check
)

# Replace execution block
old_exec_block = '''            # FIX 4: Wrap execution in try-except with safe fallback
            try:
                # Execute using current pipeline
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
                deregister_df(run_id)'''

new_exec_block = '''            # FIX 4: Wrap execution in try-except with safe fallback
            try:
                # TASK 3: BACKEND SWITCHING
                # Execute using correct backend abstraction
                if upload_session.get("use_postgres"):
                    backend = PostgresBackend()
                else:
                    backend = PandasBackend()
                
                trace.backend_used = backend.name

                dashboard_plan_dict = {
                    **asdict(plan),
                    "_meta": {
                        "kpi_count": len(plan.kpis),
                        "chart_count": len(plan.charts),
                    },
                }

                adaptive_res = backend.execute(
                    plan=exec_plan,
                    intent=intent,
                    dashboard_plan=dashboard_plan_dict,
                    df=df,
                    prev_state=prev_state,
                    session_id=session_id
                )
                
                # Check if it fell back or executed purely in postgres
                if hasattr(adaptive_res, "mode_used") and adaptive_res.mode_used == "postgres":
                    trace.backend_used = "postgres"
                
                result_state = adaptive_res.pipeline_result'''

content = content.replace(old_exec_block, new_exec_block)

# Replace result construction & caching
old_result_block = '''            result = OrchestratorResult(
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
                warnings=result_state.get("warnings") or [],
                errors=result_state.get("errors") or [],
                trace=trace.to_dict(),
            )

            # Check for partial data resolution (INCOMPLETE)
            if not result.data and not result.charts:
                result.status = "INCOMPLETE"
                if "No data generated" not in result.warnings:
                    result.warnings.append("System understood intent, but execution produced no data/charts.")

            return result'''

new_result_block = '''            from services.cache import get_llm_key
            trace.llm_cache_hit = get_llm_key(query) in llm_cache

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
                warnings=result_state.get("warnings") or [],
                errors=result_state.get("errors") or [],
                trace=trace.to_dict(),
            )

            # Check for partial data resolution (INCOMPLETE)
            if not result.data and not result.charts:
                result.status = "INCOMPLETE"
                if "No data generated" not in result.warnings:
                    result.warnings.append("System understood intent, but execution produced no data/charts.")

            # Append to cache
            if result.status == "RESOLVED":
                query_cache[q_key] = result

            return result'''

content = content.replace(old_result_block, new_result_block)

codecs.open(filepath, 'w', 'utf-8').write(content)
print("done")
