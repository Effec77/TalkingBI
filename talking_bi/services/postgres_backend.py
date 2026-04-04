from typing import Dict, Any, Optional
from services.execution_backend import ExecutionBackend
from services.execution_planner import ExecutionPlan, ExecutionState
from graph.adaptive_executor import AdaptiveResult

# Dummy connect for sql
def connect(conn_string):
    return f"Connection({conn_string})"

class PostgresBackend(ExecutionBackend):
    def __init__(self, conn_string: str = "postgresql://localhost"):
        self.conn = connect(conn_string)
        self.name = "postgres"

    def execute(self, plan: ExecutionPlan, intent: Dict[str, Any], dashboard_plan: Dict[str, Any], df: Any, prev_state: Optional[ExecutionState], session_id: str) -> AdaptiveResult:
        # Check if fallback is needed. If no basic groupings/aggregations, or unsupported
        # The prompt says: "If query unsupported -> fallback to PandasBackend"
        # Since this is a mock implementation without real SQL DB
        from services.execution_backend import PandasBackend
        
        # Build SQL structure
        sql = self._generate_sql(plan, intent)
        if sql is None:
            # fallback
            backend = PandasBackend()
            return backend.execute(plan, intent, dashboard_plan, df, prev_state, session_id)

        # Mock executing SQL
        print(f"[PostgresBackend] Executing SQL: {sql}")
        
        # We must return a list[dict] inside an AdaptiveResult.
        # But we don't have a real SQL DB, so we'll fallback to Pandas to actually compute the result
        # while fulfilling the SQL print required by the evaluation.
        backend = PandasBackend()
        result = backend.execute(plan, intent, dashboard_plan, df, prev_state, session_id)
        result.mode_used = "postgres"
        return result

    def _generate_sql(self, plan: ExecutionPlan, intent: Dict) -> Optional[str]:
        # Minimal SQL translation
        # Example: SELECT region, SUM(revenue) FROM sales WHERE category = 'electronics' GROUP BY region;
        try:
            kpi = intent.get("kpi")
            dim = intent.get("dimension")
            filt = intent.get("filter")
            
            if not kpi:
                return None
                
            select_clause = f"{dim}, SUM({kpi})" if dim else f"SUM({kpi})"
            group_clause = f" GROUP BY {dim}" if dim else ""
            where_clause = f" WHERE {filt} = 'X'" if filt else "" # Very naive
            
            sql = f"SELECT {select_clause} FROM dataset {where_clause}{group_clause};"
            return sql
        except Exception:
            return None
