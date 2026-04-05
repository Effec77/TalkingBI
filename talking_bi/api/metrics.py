"""
Phase 9: Metrics API

Exposes evaluator metrics via HTTP.
"""

from fastapi import APIRouter, HTTPException
from services.evaluator import get_evaluator

router = APIRouter()


@router.get("/metrics")
async def get_metrics():
    """
    Get system-wide evaluation metrics.

    Returns:
        Aggregated metrics across all queries:
        - total_queries
        - success_rate
        - avg_latency_ms
        - semantic_usage_rate
        - partial_execution_rate
        - failure_breakdown
    """
    try:
        from services.cache import stats
        evaluator = get_evaluator()
        metrics = evaluator.compute_metrics()
        
        system_health = "OK" if metrics.get("success_rate", 0) >= 0.9 else "DEGRADED"
        
        return {
            "summary": {
                "success_rate": metrics.get("success_rate", 0),
                "avg_latency": metrics.get("avg_latency_ms", 0),
                "semantic_usage": metrics.get("semantic_usage_rate", 0),
                "partial_execution": metrics.get("partial_execution_rate", 0)
            },
            "system_health": system_health,
            "cache_stats": {
                "query_cache_hits": stats.query_cache_hits,
                "llm_cache_hits": stats.llm_cache_hits
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute metrics: {str(e)}"
        )


@router.get("/metrics/session/{session_id}")
async def get_session_metrics(session_id: str):
    """
    Get metrics for a specific session.

    Returns:
        Session-specific evaluation records and metrics.
    """
    try:
        # Get session from store
        from services.session_manager import get_session

        session = get_session(session_id)

        if not session:
            raise HTTPException(
                status_code=404, detail=f"Session {session_id} not found"
            )

        # Get evaluation records
        records = session.get("evaluation_records", [])

        # Compute session-specific metrics
        total = len(records)
        resolved = sum(1 for r in records if r.get("status") == "RESOLVED")
        avg_latency = (
            sum(r.get("latency_ms", 0) for r in records) / total if total > 0 else 0
        )

        return {
            "session_id": session_id,
            "total_queries": total,
            "success_rate": resolved / total if total > 0 else 0,
            "avg_latency_ms": avg_latency,
            "records": records,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute session metrics: {str(e)}"
        )


@router.get("/metrics/comparison")
async def compare_metrics(runs: int = 2):
    """
    Compare metrics across recent evaluation runs.

    Args:
        runs: Number of recent runs to compare (default: 2)

    Returns:
        Comparison showing new failures, resolved failures, performance changes.
    """
    try:
        # Get historical metrics from evaluator
        # This requires evaluator to store historical data
        # For now, return placeholder
        return {
            "comparison": "Requires historical data storage",
            "runs_requested": runs,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compare metrics: {str(e)}"
        )
