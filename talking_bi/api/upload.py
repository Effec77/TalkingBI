from fastapi import APIRouter, UploadFile, File, HTTPException
import pandas as pd
from typing import Dict, List
import os
from dotenv import load_dotenv

from models.contracts import UploadedDataset
from services.session_manager import create_session

load_dotenv()

router = APIRouter()

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", 10))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


def extract_metadata(df: pd.DataFrame, filename: str, session_id: str) -> UploadedDataset:
    """Extract metadata from DataFrame and create UploadedDataset contract."""
    
    # Extract dtypes
    dtypes = {col: str(df[col].dtype) for col in df.columns}
    
    # Calculate missing percentages
    missing_pct = {col: float(df[col].isna().mean()) for col in df.columns}
    
    # Extract sample values (first 3 unique non-null values)
    sample_values = {
        col: df[col].dropna().astype(str).unique()[:3].tolist()
        for col in df.columns
    }
    
    return UploadedDataset(
        session_id=session_id,
        filename=filename,
        columns=list(df.columns),
        dtypes=dtypes,
        shape=df.shape,
        sample_values=sample_values,
        missing_pct=missing_pct
    )


@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
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
        from io import BytesIO
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
    
    # Extract metadata first (before creating session)
    session_id = str(__import__('uuid').uuid4())  # Generate ID early
    dataset = extract_metadata(df, file.filename, session_id)
    
    # Create session with metadata
    session_id = create_session(df, dataset)
    
    print(f"[UPLOAD] Session created: {session_id}, shape={df.shape}")
    
    # Return response in strict format
    return {
        "session_id": session_id,
        "dataset": {
            "filename": dataset.filename,
            "shape": list(dataset.shape),
            "columns": dataset.columns,
            "missing_pct": dataset.missing_pct
        }
    }
