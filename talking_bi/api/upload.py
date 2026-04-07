from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Depends
import pandas as pd
from typing import Dict, List
import os
from dotenv import load_dotenv
from io import BytesIO

from models.contracts import UploadedDataset
from services.session_manager import create_session
from services.dataset_awareness import (
    build_dataset_summary,
    generate_human_summary,
)
from services.dashboard_generator import generate_auto_dashboard
from services.insight_engine import generate_insights
from services.query_suggester import generate_suggestions
from services.cache import query_cache
from auth.dependencies import get_current_user_optional

load_dotenv()

router = APIRouter()

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", 200))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
PROFILE_SAMPLE_MAX_ROWS = int(os.getenv("PROFILE_SAMPLE_MAX_ROWS", 200000))


@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    mode: str = Query(default="both", pattern="^(dashboard|query|both)$"),
    user=Depends(get_current_user_optional),
):
    """
    Upload a CSV file and create a session.
    
    Returns session_id and dataset metadata.
    """
    
    print(f"[UPLOAD] File received: {file.filename}")
    
    # Validate file extension
    if not file.filename.endswith('.csv'):
        print(f"[UPLOAD] Error: Invalid file type - {file.filename}")
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .csv files are allowed."
        )
    
    # Read file content
    try:
        content = await file.read()
    except Exception as e:
        print(f"[UPLOAD] Error: Failed to read file - {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read file: {str(e)}"
        )
    
    # Validate file size
    if len(content) > MAX_FILE_SIZE_BYTES:
        print(f"[UPLOAD] Error: File too large - {len(content)} bytes")
        raise HTTPException(
            status_code=413,
            detail=f"File size exceeds maximum allowed size of {MAX_FILE_SIZE_MB}MB."
        )
    
    # Load CSV into DataFrame
    try:
        df = pd.read_csv(BytesIO(content))
    except pd.errors.EmptyDataError:
        print(f"[UPLOAD] Error: CSV file is empty")
        raise HTTPException(
            status_code=400,
            detail="CSV file is empty."
        )
    except Exception as e:
        print(f"[UPLOAD] Error: Failed to parse CSV - {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse CSV: {str(e)}"
        )
    
    # Validate DataFrame is not empty
    if df.empty:
        print(f"[UPLOAD] Error: CSV contains no data")
        raise HTTPException(
            status_code=400,
            detail="CSV file contains no data."
        )
    
    # Normalize column names
    df.columns = [
        col.strip().lower().replace(" ", "_")
        for col in df.columns
    ]
    
    # Create Advanced Profile (9C.3 Upgrade)
    from services.dataset_intelligence import DatasetIntelligence
    profile_df = df
    profile_mode = "full"
    if len(df) > PROFILE_SAMPLE_MAX_ROWS:
        # Deterministic sampling for large datasets keeps upload responsive.
        profile_df = (
            df.sample(n=PROFILE_SAMPLE_MAX_ROWS, random_state=42)
            .sort_index()
            .reset_index(drop=True)
        )
        profile_mode = "sampled"

    profile = DatasetIntelligence(profile_df).build()
    dataset_summary = build_dataset_summary(profile_df, profile)
    dataset_summary_text = generate_human_summary(dataset_summary)
    dashboard = {"kpis": [], "charts": [], "insights": [], "primary_insight": None, "fallback": None}
    suggestions_payload = generate_suggestions(profile)
    if mode in ("dashboard", "both"):
        dashboard = generate_auto_dashboard(df, profile)
        insight_payload = generate_insights(df, profile, dashboard)
        dashboard["primary_insight"] = insight_payload.get("primary_insight")
        dashboard["insights"] = insight_payload.get("insights", [])
    
    # Extract metadata for session compatibility
    session_id = str(__import__('uuid').uuid4())  # Generate ID early
    dtypes = {col: str(df[col].dtype) for col in df.columns}
    missing_pct = {col: float(df[col].isna().mean()) for col in df.columns}
    sample_values = {
        col: (
            df[col]
            .dropna()
            .astype(str)
            .head(5000)
            .unique()[:3]
            .tolist()
        )
        for col in df.columns
    }
    
    dataset = UploadedDataset(
        session_id=session_id,
        filename=file.filename,
        columns=list(df.columns),
        dtypes=dtypes,
        shape=df.shape,
        sample_values=sample_values,
        missing_pct=missing_pct
    )
    
    # Create session with metadata
    user_id = str(getattr(user, "id", "public"))
    org_id = getattr(user, "org_id", None)
    session_id = create_session(df, user_id, org_id, metadata=dataset)
    
    # Store intelligence profile directly on the active session
    from services.session_manager import get_session
    session = get_session(session_id)
    if session:
        session["dil_profile"] = profile
        session["dataset_summary"] = dataset_summary
        session["dataset_summary_text"] = dataset_summary_text
        session["dashboard"] = dashboard
        session["suggestions"] = suggestions_payload
        session["app_mode"] = mode
        session["profile_mode"] = profile_mode
        session["profile_row_count"] = len(profile_df)

    # Invalidate stale cross-session query cache entries for deterministic behavior
    # after parser/suggester upgrades and dataset re-uploads with same filename.
    query_cache.clear()
    
    print(f"[UPLOAD] Session created: {session_id}, shape={df.shape}")
    
    # Map profile to Phase 10 API expected format
    columns_output = {}
    for col_name, col_meta in profile.items():
        semantic = col_meta.get("semantic_type")
        if semantic == "kpi":
            type_str = "numeric"
        elif semantic == "dimension":
            type_str = "categorical"
        elif semantic == "date":
            type_str = "datetime"
        else:
            type_str = "unknown"
            
        columns_output[col_name] = {
            "type": type_str,
            "null_pct": col_meta.get("null_pct", 0.0),
            "unique": col_meta.get("unique", 0),
            "sample_values": col_meta.get("sample_values", []),
            "stats": col_meta.get("distribution", {})
        }
    
    # Return response in strict format
    return {
        "dataset_id": session_id,
        "columns": columns_output,
        "row_count": len(df),
        "profile_row_count": len(profile_df),
        "profile_mode": profile_mode,
        "profile": profile,
        "mode": mode,
        "dataset_summary": dataset_summary,
        "dataset_summary_text": dataset_summary_text,
        "dashboard": dashboard,
        "suggestions": {
            "type": suggestions_payload.get("type", "initial"),
            "items": suggestions_payload.get("items", []),
        },
    }
