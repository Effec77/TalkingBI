"""
Intent Validator - Phase 6B (FINAL HARDENING)

Validates that parsed intents refer to real entities in the dataset.
Prevents hallucinated column names and KPIs from reaching the pipeline.
"""

from typing import List, Dict, Any, Optional
from models.intent import VALID_INTENTS


def _normalize(text: str) -> str:
    """Normalize text for comparison (case-insensitive)."""
    return text.lower().strip() if text else ""


def validate_intent(
    intent: Dict[str, Any],
    dataset_columns: List[str],
    kpi_candidates: List[Dict[str, Any]],
) -> tuple[bool, Optional[str]]:
    """
    Validate that parsed intent references real entities.

    Args:
        intent: Parsed intent dictionary
        dataset_columns: Available column names from dataset
        kpi_candidates: ALL KPI candidates (not just selected) for validation

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if intent is valid and executable
        - error_message: None if valid, explanation if invalid

    Rules:
        1. Intent must be in VALID_INTENTS
        2. KPI (if specified) must exist in ALL candidates (not just selected)
        3. Dimension (if specified) must exist in dataset columns
        4. UNKNOWN intent is always valid (triggers clarification)
    """
    # Extract intent type
    intent_type = intent.get("intent", "UNKNOWN")

    # Rule 1: Intent must be valid
    if intent_type not in VALID_INTENTS:
        error_msg = f"Invalid intent: '{intent_type}'. Must be one of: {', '.join(sorted(VALID_INTENTS))}"
        print(f"[INTENT_VALIDATOR] {error_msg}")
        return False, error_msg

    # UNKNOWN intent is always valid (system will ask for clarification)
    if intent_type == "UNKNOWN":
        return True, None

    # Extract KPI name
    kpi_name = intent.get("kpi")

    # Rule 2: KPI must exist in ALL candidates (not just selected KPIs)
    # ✅ CRITICAL FIX: This enables datasets with 10+ KPIs
    # ✅ FIX 2: Case-insensitive matching
    if kpi_name is not None:
        # Get list of ALL valid KPI candidate names (normalized)
        candidate_names = {_normalize(k.get("name", "")) for k in kpi_candidates}

        # Also create mapping from normalized to original for return
        name_map = {
            _normalize(k.get("name", "")): k.get("name", "") for k in kpi_candidates
        }

        if _normalize(kpi_name) not in candidate_names:
            original_names = [k.get("name", "") for k in kpi_candidates]
            error_msg = f"invalid_kpi"
            print(
                f"[INTENT_VALIDATOR] KPI '{kpi_name}' not found in {len(candidate_names)} candidates"
            )
            return False, error_msg

        # Normalize KPI name in intent to match candidate
        intent["kpi"] = name_map.get(_normalize(kpi_name), kpi_name)

    # Extract dimension
    dimension = intent.get("dimension")

    # Rule 3: Dimension must exist if specified
    if dimension is not None:
        # Case-insensitive matching (LLM might capitalize differently)
        column_lower = {col.lower(): col for col in dataset_columns}

        if dimension.lower() not in column_lower:
            error_msg = f"Column '{dimension}' not found. Available: {', '.join(sorted(dataset_columns))}"
            print(f"[INTENT_VALIDATOR] {error_msg}")
            return False, error_msg

        # Normalize to actual column name (preserves original case)
        intent["dimension"] = column_lower[dimension.lower()]

    # Intent is valid
    return True, None


def get_clarification_message(intent: Dict[str, Any], error: str) -> str:
    """
    Generate a clarification message for invalid intents.

    Args:
        intent: The invalid intent
        error: Validation error message

    Returns:
        User-friendly message asking for clarification
    """
    intent_type = intent.get("intent", "UNKNOWN")

    if intent_type == "UNKNOWN":
        return (
            "I couldn't understand your query. "
            "Try asking about:\n"
            "- Explaining a trend (e.g., 'why did revenue drop?')\n"
            "- Segmenting by dimension (e.g., 'show by region')\n"
            "- Filtering (e.g., 'show Q3 only')\n"
            "- Comparing (e.g., 'compare this month vs last')\n"
            "- Top N (e.g., 'top 5 products')"
        )

    return (
        f"I understood you want to {intent_type}, but {error}. "
        "Please check your query and try again."
    )
