import codecs
import json

# 1. FIX 1: Add attributes to contracts.py
contracts_path = 'models/contracts.py'
contracts = codecs.open(contracts_path, 'r', 'utf-8').read()

kpi_val_chunk = '''    # Phase 9B: Production Execution
    backend_used: str = "pandas"
    cache_hit: bool = False
    llm_cache_hit: bool = False
    fallback_triggered: bool = False

    # Fixes
    kpi_validation: Dict[str, Any] = field(default_factory=dict)
    cache_used: bool = False
    cache_reason: str = ""'''

if 'kpi_validation' not in contracts:
    contracts = contracts.replace(
        '''    # Phase 9B: Production Execution
    backend_used: str = "pandas"
    cache_hit: bool = False
    llm_cache_hit: bool = False
    fallback_triggered: bool = False''',
        kpi_val_chunk
    )
    
    contracts = contracts.replace(
        '''            "fallback_triggered": self.fallback_triggered,
        }''',
        '''            "fallback_triggered": self.fallback_triggered,
            "kpi_validation": self.kpi_validation,
            "cache_used": self.cache_used,
            "cache_reason": self.cache_reason,
        }'''
    )
codecs.open(contracts_path, 'w', 'utf-8').write(contracts)


# 2. Updating Orchestrator cache logic, Fix 4 Normalization, Fix 6 Postgres Disable
orch_path = 'services/orchestrator.py'
orch = codecs.open(orch_path, 'r', 'utf-8').read()

# Fix 4: Normalization BEFORE things
orch = orch.replace(
    '''            q_key = get_query_key(query, metadata.filename)
            if q_key in query_cache:
                print(f"[ORCHESTRATOR] ⚡ Query cache hit!")''',
    '''            from services.query_normalizer import create_normalizer
            norm_pre = create_normalizer(metadata.columns, [])
            norm_q, _ = norm_pre.normalize(query)
            q_key = get_query_key(norm_q, metadata.filename)
            
            from services.cache import USE_CACHE
            if USE_CACHE and q_key in query_cache:
                print(f"[ORCHESTRATOR] ⚡ Query cache hit!")'''
)

# Fix 1: KPI Validation guarantee tracer.
# Find `intent = schema_mapper.map_intent(intent)`
# Wait, let's inject after:
kpi_trace = '''            intent = schema_mapper.map_intent(intent)
            
            # FIX 1: KPI Validation debug trace
            trace.kpi_validation = {
                "input": intent.get("kpi"),
                "matched_column": intent.get("kpi") if intent.get("kpi") in df.columns else None,
                "columns_available": list(df.columns)
            }'''
orch = orch.replace(
    '''            intent = schema_mapper.map_intent(intent)''',
    kpi_trace
)


# Fix 3: Pandas Source of truth, Fix 6 Disable Postgres
new_backend = '''                # Execute using correct backend abstraction
                USE_POSTGRES = False
                if USE_POSTGRES and upload_session.get("use_postgres"):
                    backend = PostgresBackend()
                    adaptive_res_pg = backend.execute(
                        plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                        df=df, prev_state=prev_state, session_id=session_id
                    )
                    backend_pd = PandasBackend()
                    adaptive_res_pd = backend_pd.execute(
                        plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                        df=df, prev_state=prev_state, session_id=session_id
                    )
                    # Check consistency
                    if str(adaptive_res_pg.final_output) == str(adaptive_res_pd.final_output):
                        adaptive_res = adaptive_res_pg
                    else:
                        adaptive_res = adaptive_res_pd
                        trace.fallback_triggered = True
                else:
                    backend = PandasBackend()
                    adaptive_res = backend.execute(
                        plan=exec_plan, intent=intent, dashboard_plan=dashboard_plan_dict,
                        df=df, prev_state=prev_state, session_id=session_id
                    )'''
orch = orch.replace(
    '''                # Execute using correct backend abstraction
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
                )''',
        '''                dashboard_plan_dict = {
                    **asdict(plan),
                    "_meta": {
                        "kpi_count": len(plan.kpis),
                        "chart_count": len(plan.charts),
                    },
                }
''' + new_backend + '''
                trace.backend_used = backend.name'''
)


# FIX 2 & 5: Cache safety mapping & partial query overriding
result_block = '''            # Check for partial data resolution (INCOMPLETE)
            if not result.data and not result.charts:
                # FIX 5: context inherited partials
                if "by " in query.lower() and trace.context_kpi_inherited:
                    pass # Keep RESOLVED if context inherited handled it
                else:
                    result.status = "INCOMPLETE"
                    if "No data generated" not in result.warnings:
                        result.warnings.append("System understood intent, but execution produced no data/charts.")

            # FIX 2: Check cache safety
            from services.cache import USE_CACHE
            if USE_CACHE:
                intent_complete = result.intent.get("kpi") is not None
                semantic_confidence = result.semantic_meta.get("confidence", 1.0)
                if result.status == "RESOLVED" and intent_complete and semantic_confidence >= 0.7:
                    query_cache[q_key] = result
                    trace.cache_used = True
                    trace.cache_reason = "Valid resolution"
                else:
                    trace.cache_used = False
                    trace.cache_reason = "Safety threshold not met"
            else:
                trace.cache_used = False
                trace.cache_reason = "Cache disabled"

            return result'''
            
orch = orch.replace(
    '''            # Check for partial data resolution (INCOMPLETE)
            if not result.data and not result.charts:
                result.status = "INCOMPLETE"
                if "No data generated" not in result.warnings:
                    result.warnings.append("System understood intent, but execution produced no data/charts.")

            # Append to cache
            if result.status == "RESOLVED":
                query_cache[q_key] = result

            return result''',
    result_block
)

codecs.open(orch_path, 'w', 'utf-8').write(orch)

print("Script modifications complete.")
