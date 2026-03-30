"""
Conversation Manager - Phase 6A (FINAL PATCH)

Manages conversation sessions for stateful interaction.
Stores run history, active pipeline state, and conversation turns.
"""

import copy
import uuid
from typing import Dict, Any, Optional
from datetime import datetime


class ConversationSession:
    """
    Represents a conversation session with stateful context.

    Attributes:
        session_id: Original upload session identifier
        run_history: List of all pipeline execution results
        active_state: Most recent PipelineState (for refinement)
        conversation_turns: Raw natural language queries from user with run linkage
        created_at: Session creation timestamp
        last_updated: Last activity timestamp
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.run_history: list = []
        self.active_state: Optional[dict] = None
        self.conversation_turns: list = []
        self.created_at = datetime.now().isoformat()
        self.last_updated = self.created_at

    def update(self, state: dict, query: str):
        """Update session with new pipeline run."""
        # FIX 1: Deep copy to prevent mutation bugs
        state_copy = copy.deepcopy(state)

        self.active_state = state_copy
        self.run_history.append(state_copy)

        # FIX 2: Structured conversation turn with query ↔ result binding
        self.conversation_turns.append(
            {"query": query, "run_id": state_copy.get("run_id")}
        )

        self.last_updated = datetime.now().isoformat()

        # FIX 7: History integrity check
        assert len(self.run_history) == len(self.conversation_turns), (
            "History mismatch: run_history and conversation_turns out of sync"
        )

    def get_stats(self) -> dict:
        """Return session statistics."""
        return {
            "session_id": self.session_id,
            "total_turns": len(self.conversation_turns),
            "total_runs": len(self.run_history),
            "created_at": self.created_at,
            "last_updated": self.last_updated,
            "has_active_state": self.active_state is not None,
        }


class ConversationManager:
    """
    Singleton manager for all conversation sessions.
    Stores sessions in memory (tied to session lifecycle).
    """

    def __init__(self):
        self._sessions: Dict[str, ConversationSession] = {}

    def get_or_create(self, session_id: str) -> ConversationSession:
        """Get existing session or create new one."""
        if session_id not in self._sessions:
            self._sessions[session_id] = ConversationSession(session_id)
        return self._sessions[session_id]

    def get_session(self, session_id: str) -> Optional[ConversationSession]:
        """Get existing session or None."""
        return self._sessions.get(session_id)

    def update_session(self, session_id: str, state: dict, query: str):
        """Update session with new state and query."""
        session = self.get_or_create(session_id)
        session.update(state, query)

    def clear_session(self, session_id: str):
        """Clear session (call on session expiry)."""
        if session_id in self._sessions:
            del self._sessions[session_id]

    def get_all_stats(self) -> Dict[str, dict]:
        """Get stats for all sessions (for monitoring)."""
        return {sid: session.get_stats() for sid, session in self._sessions.items()}


# Global instance (similar to session_manager pattern)
_conversation_manager = None


def get_conversation_manager() -> ConversationManager:
    """Get or create global conversation manager instance."""
    global _conversation_manager
    if _conversation_manager is None:
        _conversation_manager = ConversationManager()
    return _conversation_manager
