import codecs
import re
import os

# Phase 9C Main Orchestrator Hardening

def patch_orchestrator_9c():
    path = 'services/orchestrator.py'
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return
    content = codecs.open(path, 'r', 'utf-8').read()

    # Rule 5: Cache Integrity (Update cache key and context bypass)
    # We move cache lookup after context loading or keep it early but with session/context data
    # Current cache logic is around line 90.
    
    # Update cache lookup to be more robust
    cache_lookup_old = '''            # TASK 5: QUERY RESULT CACHE (High Impact)
            from services.query_normalizer import create_normalizer
            norm_pre = create_normalizer(metadata.columns, [])
            norm_q, _ = norm_pre.normalize(query)
            q_key = get_query_key(norm_q, metadata.filename)
            
            from services.cache import USE_CACHE
            if USE_CACHE and q_key in query_cache:'''
            
    cache_lookup_new = '''            # TASK 5: QUERY RESULT CACHE (High Impact)
            from services.query_normalizer import create_normalizer
            norm_pre = create_normalizer(metadata.columns, [])
            norm_q, _ = norm_pre.normalize(query)
            
            # Get conversation context for cache key
            conv_session_cache = self.conv_manager.get_or_create(session_id)
            context_history_cache = conv_session_cache.run_history if conv_session_cache else []
            last_intent_cache = context_history_cache[-1].get("intent") if context_history_cache else None
            
            q_key = get_query_key(norm_q, metadata.filename, context=last_intent_cache)
            
            from services.cache import USE_CACHE
            if USE_CACHE and q_key in query_cache:'''

    content = content.replace(cache_lookup_old, cache_lookup_new)

    # Task 8: Confidence Scoring & Failure Intelligence
    # Inject confidence calculation before validation check
    
    confidence_logic = '''            # Task 8: Confidence Scoring (Phase 9C)
            kpi_term = intent.get("kpi")
            dim_term = intent.get("dimension")
            
            kpi_conf = 1.0 if kpi_term in df.columns else 0.5 if kpi_term else 0.0
            sem_conf = trace.semantic_confidence or 0.0
            ctx_conf = 1.0 if trace.context_used else 0.0
            
            overall_conf = max(kpi_conf, sem_conf, ctx_conf)
            if not kpi_term and not dim_term:
                overall_conf = 0.0
                
            trace.confidence = {
                "kpi": kpi_conf,
                "semantic": sem_conf,
                "context": ctx_conf,
                "overall": overall_conf
            }
            
            # Rule 8: If overall confidence is low, mark as INCOMPLETE/UNKNOWN
            if overall_conf < 0.5 and intent.get("intent") != "UNKNOWN":
                print(f"[ORCHESTRATOR] ⚠ Low confidence query ({overall_conf}), marking INCOMPLETE")
                trace.failure_reason = {
                    "type": "SEMANTIC_FAIL",
                    "stage": "7",
                    "details": "Low confidence in intent resolution"
                }
                return self._unresolved_result(
                    query, session_id, intent, 
                    type('Res', (object,), {"status": "INCOMPLETE", "intent": intent, "warnings": [type('Warn', (object,), {"type": "confidence", "message": "Low confidence query"})], "context_used": False, "context_applied": False})(),
                    trace, start_time
                )'''

    # Injecting before Step 7 schema mapping or Step 8 validation
    # Actually, after Step 7 Schema map is better.
    
    content = content.replace(
        '# Step 7: Schema map (6F)',
        confidence_logic + '\n\n            # Step 7: Schema map (6F)'
    )

    # Failure Intelligence: Update _record_evaluator to pass failure reason if needed
    # (Evaluator.record already takes 'result', and 'result' has 'trace', and 'trace' has 'failure_reason')
    
    # Task 3: Context validation in Orchestrator call
    content = content.replace(
        'resolution_result = resolver.resolve(intent, dashboard_plan_dict)',
        'resolution_result = resolver.resolve(intent, dashboard_plan_dict, current_columns=list(df.columns))'
    )

    # Task 6: Backend Consistency Check (Dev Only)
    backend_logic_old = '''                USE_POSTGRES = False
                if USE_POSTGRES and upload_session.get("use_postgres"):'''
                
    backend_logic_new = '''                # Task 6: Backend Consistency (Phase 9C)
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
                else:'''

    content = content.replace(backend_logic_old, backend_logic_new)
    
    # Ensure failure reason is captured for results
    # Add to _unresolved_result and _invalid_result if necessary, 
    # but the trace is already passed into them.
    
    codecs.open(path, 'w', 'utf-8').write(content)
    print("Patched orchestrator.py (9C Phase 2)")

if __name__ == "__main__":
    patch_orchestrator_9c()
    print("Orchestrator 9C hardening complete.")
