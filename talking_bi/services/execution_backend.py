"""
Phase 9: Execution Backend Abstraction

Provides backend-agnostic execution interface.
Phase 9A: PandasBackend (default)
Phase 9B: PostgresBackend (future)
"""

from typing import Protocol, Dict, Any, Optional, List
from dataclasses import dataclass
from uuid import uuid4

from services.execution_planner import ExecutionPlan, ExecutionState
from graph.adaptive_executor import adaptive_execute, AdaptiveResult
from graph.df_registry import register_df, deregister_df


class ExecutionBackend(Protocol):
    """
    Protocol for execution backends.

    Implementations:
    - PandasBackend (Phase 9A): Uses pandas + optional LangGraph
    - PostgresBackend (Phase 9B): Uses SQL generation

    The ExecutionPlan must remain backend-agnostic.
    """

    def execute(
        self,
        plan: ExecutionPlan,
        intent: Dict[str, Any],
        dashboard_plan: Dict[str, Any],
        df: Any,  # pd.DataFrame for PandasBackend
        prev_state: Optional[ExecutionState],
        session_id: str,
    ) -> AdaptiveResult:
        """
        Execute an execution plan.

        Args:
            plan: Backend-agnostic execution plan
            intent: Resolved intent with KPI, dimension, filter
            dashboard_plan: Generated dashboard plan
            df: Dataset (type depends on backend)
            prev_state: Previous execution state for caching
            session_id: Session identifier

        Returns:
            AdaptiveResult with output and cacheable state
        """
        ...


@dataclass
class ExecutionContext:
    """Context for execution backend."""

    session_id: str
    run_id: str
    intent: Dict[str, Any]
    dashboard_plan: Dict[str, Any]


class PandasBackend:
    """
    Phase 9A default backend.

    Uses:
    - FULL_RUN: LangGraph pipeline (query → prep → insight → chart)
    - PARTIAL_RUN: Pure pandas operations (reuse cached dataframes)
    """

    def __init__(self):
        self.name = "pandas"

    def execute(
        self,
        plan: ExecutionPlan,
        intent: Dict[str, Any],
        dashboard_plan: Dict[str, Any],
        df: Any,
        prev_state: Optional[ExecutionState],
        session_id: str,
    ) -> AdaptiveResult:
        """
        Execute using pandas-based adaptive executor.

        This wraps the existing adaptive_execute() function
        from Phase 6D.
        """
        run_id = str(uuid4())

        # Register DataFrame for pipeline access
        from graph.df_registry import register_df, deregister_df

        register_df(run_id, df)

        try:
            # Call existing adaptive executor from Phase 6D
            result = adaptive_execute(
                plan=plan,
                resolved_intent=intent,
                dashboard_plan=dashboard_plan,
                df=df,
                prev_state=prev_state,
                session_id=session_id,
                run_id=run_id,
            )

            return result

        finally:
            # Cleanup
            deregister_df(run_id)

    def __repr__(self) -> str:
        return f"PandasBackend(mode=FULL_RUN|PARTIAL_RUN)"


class PostgresBackend:
    """
    Phase 9B backend for SQL-based execution.

    Not implemented in Phase 9A - placeholder for future.

    Will:
    - Translate ExecutionPlan to SQL
    - Execute against PostgreSQL
    - Return same AdaptiveResult shape
    """

    def __init__(self, connection_string: str):
        self.name = "postgres"
        self.connection_string = connection_string
        raise NotImplementedError("PostgresBackend in Phase 9B")

    def execute(
        self,
        plan: ExecutionPlan,
        intent: Dict[str, Any],
        dashboard_plan: Dict[str, Any],
        df: Any,  # Would be table name for Postgres
        prev_state: Optional[ExecutionState],
        session_id: str,
    ) -> AdaptiveResult:
        """
        Future: Execute using SQL generation.

        Steps:
        1. SQLGenerator(plan) → SQL query
        2. Execute against PostgreSQL
        3. Convert results to AdaptiveResult format
        """
        # Phase 9B implementation
        raise NotImplementedError("SQL generation not implemented in Phase 9A")

    def _generate_sql(self, plan: ExecutionPlan, intent: Dict) -> str:
        """Future: Generate SQL from execution plan."""
        # Phase 9B: SQL generation logic
        pass


# Backend factory
def create_backend(backend_type: str = "pandas", **kwargs) -> ExecutionBackend:
    """
    Factory function for execution backends.

    Args:
        backend_type: "pandas" or "postgres"
        **kwargs: Backend-specific configuration

    Returns:
        ExecutionBackend instance
    """
    if backend_type == "pandas":
        return PandasBackend()
    elif backend_type == "postgres":
        return PostgresBackend(**kwargs)
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")


# Singleton default backend
_default_backend: Optional[ExecutionBackend] = None


def get_default_backend() -> ExecutionBackend:
    """Get default pandas backend (lazy initialization)."""
    global _default_backend
    if _default_backend is None:
        _default_backend = PandasBackend()
    return _default_backend
