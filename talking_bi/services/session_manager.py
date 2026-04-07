import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
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


def create_session(df: pd.DataFrame, user_id: str, org_id: Optional[str] = None, metadata=None, dataset_hash: str = "") -> str:
    """
    Create a new unified session with the provided DataFrame and metadata.

    Phase 9 Unified Schema:
    - df: Raw uploaded data
    - metadata: Static dataset metadata
    - execution_state: Phase 6D cache (base_df, filtered_df, last_result, last_intent)
    - conversation: Turn history for 6C resolver
    - dashboard_plan: Generated once, reused per turn
    - dataset_hash: For cache invalidation
    - created_at, expires_at: TTL management
    """
    session_id = str(uuid.uuid4())
    now = datetime.now()
    expires_at = now + timedelta(hours=SESSION_EXPIRY_HOURS)

    SESSION_STORE[session_id] = {
        # Core data
        "df": df,
        "metadata": metadata,
        "dataset_hash": dataset_hash,
        # Phase 6D: Execution state for caching
        "execution_state": None,  # ExecutionState object
        # Phase 6C: Conversation context
        "conversation": [],  # List of conversation turns
        # Phase 0B: Dashboard plan (generated once)
        "dashboard_plan": None,
        # Phase 8: Evaluation records
        "evaluation_records": [],
        # SaaS scoping
        "user_id": user_id,
        "org_id": org_id,
        # Metadata
        "created_at": now,
        "expires_at": expires_at,
    }

    return session_id


def get_session(session_id: str) -> Optional[Dict]:
    """Retrieve a unified session by ID."""
    session = SESSION_STORE.get(session_id)

    if session is None:
        return None

    # Check if expired
    if datetime.now() > session["expires_at"]:
        delete_session(session_id)
        return None

    return session


def update_session_execution_state(session_id: str, execution_state: Any) -> bool:
    """Update the execution state (Phase 6D cache) for a session."""
    session = get_session(session_id)
    if not session:
        return False

    session["execution_state"] = execution_state
    return True


def update_session_conversation(session_id: str, turn: Dict[str, Any]) -> bool:
    """Add a conversation turn to the session."""
    session = get_session(session_id)
    if not session:
        return False

    if "conversation" not in session:
        session["conversation"] = []

    session["conversation"].append(turn)
    return True


def update_session_dashboard_plan(session_id: str, dashboard_plan: Any) -> bool:
    """Store the generated dashboard plan for a session."""
    session = get_session(session_id)
    if not session:
        return False

    session["dashboard_plan"] = dashboard_plan
    return True


def add_evaluation_record(session_id: str, record: Dict[str, Any]) -> bool:
    """Add an evaluation record (Phase 8) to the session."""
    session = get_session(session_id)
    if not session:
        return False

    if "evaluation_records" not in session:
        session["evaluation_records"] = []

    session["evaluation_records"].append(record)
    return True


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
        sid for sid, session in SESSION_STORE.items() if now > session["expires_at"]
    ]

    for sid in expired_ids:
        delete_session(sid)

    if expired_ids:
        print(f"Cleaned up {len(expired_ids)} expired session(s)")


def start_cleanup_scheduler():
    """Start the background scheduler for session cleanup."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        cleanup_expired_sessions, "interval", minutes=CLEANUP_INTERVAL_MINUTES
    )
    scheduler.start()
    return scheduler
