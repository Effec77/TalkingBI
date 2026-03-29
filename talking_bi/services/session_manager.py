import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
import os
from dotenv import load_dotenv

load_dotenv()

# In-memory session store
SESSION_STORE: Dict[str, Dict] = {}

# Configuration
SESSION_EXPIRY_HOURS = int(os.getenv("SESSION_EXPIRY_HOURS", 24))
CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", 10))


def create_session(df: pd.DataFrame, metadata=None) -> str:
    """Create a new session with the provided DataFrame and metadata."""
    session_id = str(uuid.uuid4())
    now = datetime.now()
    expires_at = now + timedelta(hours=SESSION_EXPIRY_HOURS)
    
    SESSION_STORE[session_id] = {
        "df": df,
        "metadata": metadata,
        "created_at": now,
        "expires_at": expires_at
    }
    
    return session_id


def get_session(session_id: str) -> Optional[Dict]:
    """Retrieve a session by ID."""
    session = SESSION_STORE.get(session_id)
    
    if session is None:
        return None
    
    # Check if expired
    if datetime.now() > session["expires_at"]:
        delete_session(session_id)
        return None
    
    return session


def delete_session(session_id: str) -> bool:
    """Delete a session by ID."""
    if session_id in SESSION_STORE:
        del SESSION_STORE[session_id]
        return True
    return False


def cleanup_expired_sessions():
    """Remove all expired sessions from the store."""
    now = datetime.now()
    expired_ids = [
        sid for sid, session in SESSION_STORE.items()
        if now > session["expires_at"]
    ]
    
    for sid in expired_ids:
        delete_session(sid)
    
    if expired_ids:
        print(f"Cleaned up {len(expired_ids)} expired session(s)")


def start_cleanup_scheduler():
    """Start the background scheduler for session cleanup."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        cleanup_expired_sessions,
        'interval',
        minutes=CLEANUP_INTERVAL_MINUTES
    )
    scheduler.start()
    return scheduler
