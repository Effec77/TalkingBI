from langgraph.graph import StateGraph

from .nodes import query_node, prep_node, insight_node, chart_node
from .state import PipelineState


def build_graph():
    try:
        builder = StateGraph(PipelineState)

        builder.add_node("query", query_node)
        builder.add_node("prep", prep_node)
        builder.add_node("insight", insight_node)
        builder.add_node("chart", chart_node)

        builder.set_entry_point("query")

        builder.add_edge("query", "prep")
        builder.add_edge("prep", "insight")
        builder.add_edge("insight", "chart")

        builder.set_finish_point("chart")

        return builder.compile()

    except Exception as e:
        print("[GRAPH ERROR]", str(e))
        raise
