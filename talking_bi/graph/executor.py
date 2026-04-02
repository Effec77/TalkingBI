from .graph_builder import build_graph
from .state import PipelineState

graph = None


def get_graph():
    global graph
    if graph is None:
        print("[GRAPH] Building LangGraph instance")
        graph = build_graph()
    return graph


def run_pipeline(initial_state: PipelineState) -> PipelineState:
    print("[EXECUTOR] Running pipeline")
    print(f"[EXECUTOR] run_id={initial_state.get('run_id')}")
    g = get_graph()
    result = g.invoke(initial_state)

    # ── Output validation ───────────────────────────────────────────────
    # Fail loudly if any node dropped a required key from the state.
    assert "query_results" in result, (
        "[EXECUTOR] query_results missing from final state"
    )
    assert "prepared_data" in result, (
        "[EXECUTOR] prepared_data missing from final state"
    )
    assert "insights" in result, "[EXECUTOR] insights missing from final state"
    assert "chart_specs" in result, "[EXECUTOR] chart_specs missing from final state"
    # Phase 3 validations
    assert "transformed_data" in result, (
        "[EXECUTOR] transformed_data missing from final state"
    )
    assert "execution_trace" in result, (
        "[EXECUTOR] execution_trace missing from final state"
    )
    # Phase 6B validation - intent must survive pipeline
    assert "intent" in result, (
        "[EXECUTOR] intent missing from final state - Phase 6C will fail!"
    )

    trace = result.get("execution_trace", [])
    retries = sum(
        1 for r in result.get("query_results", []) if r.get("status") == "retry_success"
    )
    has_narrative = result.get("insight_summary") is not None

    print(
        f"[EXECUTOR] Pipeline complete — "
        f"queries={len(result.get('query_results', []))}, "
        f"charts={len(result.get('chart_specs', []))}, "
        f"insights={len(result.get('insights', []))}, "
        f"narrative={'yes' if has_narrative else 'no'}, "
        f"retries={retries}, "
        f"trace={trace}"
    )
    return result
