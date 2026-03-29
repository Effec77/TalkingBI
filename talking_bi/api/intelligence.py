"""
Intelligence API - Phase 0B
Endpoint to generate dashboard plan from uploaded dataset
"""
from fastapi import APIRouter, HTTPException
from services.session_manager import get_session
from services.intelligence_engine import generate_dashboard_plan

router = APIRouter()


@router.post("/intelligence/{session_id}")
async def generate_intelligence(session_id: str):
    """
    Generate dashboard plan from uploaded dataset.
    
    Phase 0B: Dataset Intelligence
    - Profiles dataset
    - Generates KPI candidates
    - Selects KPIs with LLM
    - Validates KPIs
    - Creates dashboard plan
    
    Args:
        session_id: Session identifier from upload
        
    Returns:
        Dashboard plan with KPIs and charts
    """
    print(f"[API] Intelligence request for session: {session_id}")
    
    # Get session
    session = get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} not found or expired"
        )
    
    # Get DataFrame and metadata
    df = session['df']
    metadata = session.get('metadata')
    
    if metadata is None:
        raise HTTPException(
            status_code=400,
            detail="Session metadata not found"
        )
    
    try:
        # Generate dashboard plan
        dashboard_plan = generate_dashboard_plan(
            session_id=session_id,
            df=df,
            uploaded_dataset=metadata
        )
        
        # Convert to response format
        response = {
            "session_id": dashboard_plan.session_id,
            "kpis": [
                {
                    "name": kpi.name,
                    "source_column": kpi.source_column,
                    "aggregation": kpi.aggregation,
                    "segment_by": kpi.segment_by,
                    "time_column": kpi.time_column,
                    "business_meaning": kpi.business_meaning,
                    "confidence": kpi.confidence
                }
                for kpi in dashboard_plan.kpis
            ],
            "charts": [
                {
                    "chart_type": chart.chart_type,
                    "title": chart.title,
                    "x_column": chart.x_column,
                    "y_column": chart.y_column,
                    "kpi_name": chart.kpi_name,
                    "aggregation": chart.aggregation,
                    "segment_by": chart.segment_by
                }
                for chart in dashboard_plan.charts
            ],
            "story_arc": dashboard_plan.story_arc,
            "kpi_coverage": dashboard_plan.kpi_coverage,
            "created_at": dashboard_plan.created_at
        }
        
        print(f"[API] Intelligence generated successfully")
        return response
        
    except Exception as e:
        print(f"[API] Error generating intelligence: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate intelligence: {str(e)}"
        )
