"""
Phase 7: Semantic Intelligence Layer

Maps vague user terms (e.g., "usage", "performance", "sales") to actual
dataset KPIs -- WITHOUT LLM, WITHOUT breaking determinism.

Position in pipeline:
    6E (normalize) → 6G (deterministic) → 6B (LLM parse) → [7] → 6F (schema map) → 6C (resolve)

Activation condition:
    intent.kpi is None OR intent.intent == "UNKNOWN"

Resolution chain:
    1. semantic_map   — domain vocabulary → column name hints
    2. dataset match  — score hints against actual KPIs in dataset
    3. confidence gate — accept if score ≥ 0.7, else return unchanged

Rules:
    - NEVER override an explicitly set KPI
    - NEVER use LLM for decisions
    - NEVER remove UNKNOWN fallback
    - ALWAYS filter to KPIs that actually exist in the dataset
    - ALWAYS attach semantic_meta for observability
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────
# Semantic vocabulary map
#
# Key   = user's vague term (lowercase)
# Value = ordered list of column name hints (most specific first)
#
# These are HINTS, not direct mappings. Each hint is scored
# against the actual dataset KPIs at runtime.
# ─────────────────────────────────────────────────────────────
SEMANTIC_MAP: Dict[str, List[str]] = {
    # Revenue / financial
    "sales": [
        "revenue",
        "sales",
        "amount",
        "total_amount",
        "total_revenue",
        "net_revenue",
        "gross_revenue",
        "income",
        "turnover",
    ],
    "earnings": [
        "revenue",
        "sales",
        "net_income",
        "earnings",
        "profit",
        "net_profit",
        "gross_profit",
    ],
    "income": ["revenue", "income", "net_income", "gross_income", "earnings"],
    "revenue": [
        "revenue",
        "total_revenue",
        "net_revenue",
        "gross_revenue",
        "mrr",
        "sales",
        "amount",
    ],
    # Product / inventory
    "orders": [
        "order_count",
        "orders",
        "total_orders",
        "num_orders",
        "quantity",
        "units_sold",
    ],
    "quantity": ["quantity", "units", "units_sold", "volume", "count"],
    # Engagement / activity (SaaS / product analytics)
    "activity": ["event_count", "sessions", "logins", "active_users", "clicks"],
    "sessions": ["sessions", "session_count", "visits", "session_duration"],
    "retention": ["retention_rate", "churn_flag", "churned", "active_users"],
    "churn": ["churn_flag", "churn_rate", "churned", "cancelled"],
    # Trends
    "trend": ["revenue", "sales", "monthly_revenue", "mrr", "order_count"],
    # SaaS metrics
    "mrr": [
        "mrr",
        "monthly_recurring_revenue",
        "monthly_revenue",
        "subscription_revenue",
    ],
    "arr": ["arr", "annual_recurring_revenue", "annual_revenue"],
    "ltv": ["ltv", "lifetime_value", "customer_lifetime_value", "clv"],
    "conversion": ["conversion_rate", "conversions", "converted"],
    "subscribers": [
        "subscriber_count",
        "subscribers",
        "active_subscribers",
        "subscriptions",
    ],
    # Cost / expense
    "cost": ["cost", "expenses", "total_cost", "cogs", "spend"],
    "expenses": ["expenses", "cost", "total_cost", "spend", "cogs"],
    "profit": ["profit", "net_profit", "gross_profit", "margin", "profit_margin"],
    "margin": ["profit_margin", "gross_margin", "margin", "profit"],
    # Customers / users
    "customers": [
        "customer_count",
        "customers",
        "new_customers",
        "total_customers",
        "unique_customers",
    ],
    "users": ["user_count", "users", "active_users", "total_users", "unique_users"],
    # Finance
    "spend": ["spend", "expenses", "cost", "total_spend", "amount"],
    "budget": ["budget", "allocated", "spend", "cost"],
    "balance": ["balance", "closing_balance", "account_balance", "net_balance"],
    "assets": ["assets", "total_assets", "asset_value"],
    "liabilities": ["liabilities", "total_liabilities"],
    # ── Domain-specific (Phase 8 improvements) ────────────────────────
    # "performance" — domain-agnostic: highest-confidence column in the
    # actual dataset wins. HR → performance_score, marketing → revenue, etc.
    "performance": [
        "performance_score",
        "revenue",
        "sales",
        "profit",
        "amount",
        "total_revenue",
        "net_revenue",
        "conversion_rate",
        "output_units",
        "delivery_time",
        "feature_usage",
        "salary",
    ],
    # "growth" — domain-agnostic: covers SaaS (mrr), ecommerce (revenue), HR etc.
    "growth": [
        "mrr",
        "revenue",
        "arr",
        "sales",
        "monthly_revenue",
        "feature_usage",
        "new_customers",
        "signups",
        "output_units",
    ],
    # "volume" — generic high-cardinality metric
    "volume": [
        "quantity",
        "amount",
        "transaction_count",
        "units_sold",
        "output_units",
        "impressions",
        "order_count",
    ],
    # "transactions" — covers banking amount as primary hit
    "transactions": [
        "amount",
        "transaction_count",
        "transactions",
        "num_transactions",
        "order_count",
    ],
    # HR domain
    "workforce": ["salary", "employee_count", "headcount", "total_salary"],
    "attrition": ["attrition_flag", "churn_flag", "turnover_rate", "churned"],
    "compensation": ["salary", "total_compensation", "pay", "wages"],
    "headcount": ["employee_count", "headcount", "staff_count", "total_employees"],
    # Supply chain domain
    "efficiency": [
        "delivery_time",
        "inventory_level",
        "cycle_time",
        "throughput",
        "on_time_rate",
        "fill_rate",
    ],
    "delays": [
        "delay_flag",
        "late_deliveries",
        "overdue_count",
        "delay_rate",
        "missed_sla",
    ],
    "inventory": [
        "inventory_level",
        "stock_level",
        "units_in_stock",
        "on_hand",
        "warehouse_qty",
    ],
    "fulfillment": ["delivery_time", "on_time_rate", "fill_rate", "lead_time"],
    "lead_time": ["delivery_time", "lead_time", "cycle_time", "processing_time"],
    "shipments": ["shipment_count", "deliveries", "orders_shipped", "delivery_time"],
    # Manufacturing domain
    "production": [
        "output_units",
        "units_produced",
        "throughput",
        "production_volume",
        "units_manufactured",
    ],
    "quality": [
        "defect_count",
        "defect_rate",
        "yield",
        "scrap_rate",
        "reject_rate",
        "pass_rate",
    ],
    "downtime": [
        "downtime_minutes",
        "machine_downtime",
        "idle_time",
        "unplanned_downtime",
    ],
    "output": ["output_units", "production_volume", "units_produced", "throughput"],
    "defects": ["defect_count", "defect_rate", "reject_count", "scrap_count"],
    # Banking / finance domain
    "fraud": [
        "fraud_flag",
        "fraud_rate",
        "suspicious_count",
        "fraudulent_transactions",
    ],
    "risk": ["fraud_flag", "risk_score", "fraud_rate", "default_rate"],
    "deposits": ["deposit_amount", "deposits", "balance", "amount"],
    "withdrawals": ["withdrawal_amount", "withdrawals", "amount"],
    # SaaS — feature-level engagement
    "usage": [
        "feature_usage",
        "session_duration",
        "event_count",
        "sessions",
        "page_views",
        "active_users",
        "dau",
        "mau",
        "logins",
        "clicks",
    ],
    "engagement": [
        "feature_usage",
        "session_duration",
        "page_views",
        "event_count",
        "clicks",
        "interactions",
        "sessions",
    ],
    # Marketing domain
    "traffic": ["impressions", "clicks", "sessions", "page_views", "visitors", "reach"],
    "reach": ["impressions", "reach", "audience_size", "views"],
    "clicks": ["clicks", "click_count", "ctr", "click_through"],
    "conversions": ["conversions", "conversion_rate", "leads", "sign_ups"],
    "roi": ["revenue", "roas", "return_on_ad_spend", "profit"],
}

# Minimum confidence threshold to apply a semantic mapping
CONFIDENCE_THRESHOLD = 0.70


# ─────────────────────────────────────────────────────────────
# Scoring / matching helpers
# ─────────────────────────────────────────────────────────────


def _normalize(text: str) -> str:
    """Lowercase, strip, collapse spaces/underscores."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[\s_\-]+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "", text)
    return text


def _score_hint_against_kpis(
    hint: str,
    kpi_candidates: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], float]:
    """
    Score a single semantic hint against the available KPI candidates.

    Scoring rules:
        1.00 — exact name match (case-insensitive)
        0.92 — normalized name match
        0.85 — source_column exact match
        0.80 — normalized source_column match
        0.75 — hint is a substring of kpi name
        0.72 — kpi name is a substring of hint

    Returns (best_kpi_dict, score) or (None, 0.0) if no match.
    """
    if not hint or not kpi_candidates:
        return None, 0.0

    hint_lower = hint.lower()
    hint_norm = _normalize(hint)

    best_kpi = None
    best_score = 0.0

    for kpi in kpi_candidates:
        kpi_name = (kpi.get("name") or "").lower()
        kpi_source = (kpi.get("source_column") or "").lower()
        kpi_name_norm = _normalize(kpi_name)
        kpi_source_norm = _normalize(kpi_source)

        score = 0.0

        if hint_lower == kpi_name:
            score = 1.00
        elif hint_norm == kpi_name_norm:
            score = 0.92
        elif hint_lower == kpi_source:
            score = 0.85
        elif hint_norm == kpi_source_norm:
            score = 0.80
        elif hint_norm in kpi_name_norm:
            score = 0.75
        elif kpi_name_norm in hint_norm:
            score = 0.72

        if score > best_score:
            best_score = score
            best_kpi = kpi

    return best_kpi, best_score


def _resolve_vague_term(
    vague_term: str,
    kpi_candidates: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], float, str]:
    """
    Resolve a vague term to the best KPI candidate via semantic map.

    Steps:
        1. Look up vague_term in SEMANTIC_MAP
        2. For each hint (in priority order), score against dataset KPIs
        3. Pick best scoring match across all hints
        4. Return (kpi_dict, score, resolution_source)

    Returns:
        (kpi_dict, confidence, source)
        source: "semantic_map" | "no_match"
    """
    if not vague_term or not kpi_candidates:
        return None, 0.0, "no_match"

    term_lower = vague_term.lower().strip()
    hints = SEMANTIC_MAP.get(term_lower, [])

    if not hints:
        return None, 0.0, "no_match"

    best_kpi = None
    best_score = 0.0
    best_hint = None

    for hint in hints:
        kpi, score = _score_hint_against_kpis(hint, kpi_candidates)
        if score > best_score:
            best_score = score
            best_kpi = kpi
            best_hint = hint

    if best_kpi and best_score > 0:
        return best_kpi, best_score, f"semantic_map[{term_lower}→{best_hint}]"

    return None, 0.0, "no_match"


def _extract_vague_term(query: str, intent: Dict[str, Any]) -> Optional[str]:
    """
    Extract the vague term from query when intent.kpi is missing.

    Strategy:
        - Remove common filler words
        - Take the remaining significant token as the candidate term
    """
    if not query:
        return None

    query_lower = query.lower().strip()

    # Strip common prefixes
    prefixes = [
        "show me",
        "show",
        "display",
        "give me",
        "what is",
        "what are",
        "get",
        "fetch",
        "analyze",
        "analysis of",
        "report on",
        "report",
        "summarize",
        "summary of",
        "summary",
    ]
    for prefix in prefixes:
        if query_lower.startswith(prefix + " "):
            query_lower = query_lower[len(prefix) :].strip()
            break

    # Strip common suffixes
    suffixes = [
        "data",
        "metrics",
        "numbers",
        "stats",
        "statistics",
        "trend",
        "trends",
        "analysis",
        "report",
        "overview",
    ]
    for suffix in suffixes:
        if query_lower.endswith(" " + suffix):
            query_lower = query_lower[: -(len(suffix) + 1)].strip()
            break

    # Remove "the", "all", "total", "overall"
    stopwords = {"the", "all", "total", "overall", "our", "this", "my"}
    tokens = [t for t in query_lower.split() if t not in stopwords]

    if not tokens:
        return None

    # Return single remaining token or the full cleaned phrase
    candidate = "_".join(tokens)
    return candidate if candidate else None


class SemanticInterpreter:
    """
    Phase 7: Semantic Intelligence.

    Resolves vague KPI terms to actual dataset KPIs.

    Rules:
        - Only activates when intent.kpi is None or intent is UNKNOWN
        - Never overrides an explicit KPI set by 6G or 6B
        - Confidence gate: mapping only applied if score >= 0.7
        - Dataset-aware: only maps to KPIs that actually exist
        - Deterministic: same inputs → same outputs, no LLM
    """

    def __init__(self, df, schema_mapper):
        """
        Args:
            df:            DataFrame from upload session
            schema_mapper: SchemaMapper instance (Phase 6F)
        """
        self.df = df
        self.schema_mapper = schema_mapper
        self.columns = list(df.columns)

    def _get_kpi_candidates(self) -> List[Dict[str, Any]]:
        """Get KPI candidates from the schema mapper."""
        return self.schema_mapper.kpi_candidates

    def interpret(self, query: str, intent: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE:SEMANTIC] Input: {intent}")
        if intent.get("_locked"):
            print(f"[TRACE:SEMANTIC] LOCK ACTIVE: {intent.get('_lock_source')}")

        # Fix 1: Trend Lock Check (Phase 9C.1)
        if intent.get("_locked"):
            return intent.copy()

        """
        Attempt to resolve a vague query term to a concrete KPI.

        Args:
            query:  Original (or normalized) user query string
            intent: Intent dict from 6G or 6B parser

        Returns:
            Updated intent dict with semantic_meta field always present.
            If mapping applied: intent.kpi is set.
            If not: intent returned unchanged, semantic_meta.applied=False.

        Non-negotiable guards:
            - intent.kpi already set → return unchanged
            - intent.intent not UNKNOWN and
              intent.intent != "SEGMENT_BY"/"SUMMARIZE" → return unchanged
            - Confidence < 0.7 → return unchanged
            - No hint matches any dataset KPI → return unchanged
        """
        result = intent.copy()
        kpi_candidates = self._get_kpi_candidates()

        def _skip(reason: str) -> Dict[str, Any]:
            result["semantic_meta"] = {
                "applied": False,
                "input": query,
                "mapped_to": None,
                "confidence": 0.0,
                "reason": reason,
            }
            print(f"[7] Skipped: {reason}")
            return result

        # ── Guard 1: KPI already set → do not override ────────────────
        if intent.get("kpi"):
            return _skip("kpi already set — no override")

        # ── Guard 2: Only run on relevant intents ─────────────────────
        intent_type = intent.get("intent", "UNKNOWN")
        eligible_intents = {
            "UNKNOWN",
            "SEGMENT_BY",
            "SUMMARIZE",
            "EXPLAIN_TREND",
            "FILTER",
        }
        if intent_type not in eligible_intents:
            return _skip(f"intent={intent_type} not eligible for semantic mapping")

        # ── Guard 3: Need candidates to compare against ───────────────
        if not kpi_candidates:
            return _skip("no kpi_candidates in dataset")

        # ── Step 1: Extract vague term from query ─────────────────────
        vague_term = _extract_vague_term(query, intent)
        if not vague_term:
            return _skip("could not extract term from query")

        # Normalise to single-word if multi-token
        primary_term = vague_term.replace("_", " ").split()[0]

        # ── Step 2: Resolve via semantic map ──────────────────────────
        kpi_dict, confidence, source = _resolve_vague_term(primary_term, kpi_candidates)

        # ── Guard 4: Confidence gate ──────────────────────────────────
        if kpi_dict is None or confidence < CONFIDENCE_THRESHOLD:
            result["semantic_meta"] = {
                "applied": False,
                "input": query,
                "vague_term": primary_term,
                "mapped_to": None,
                "confidence": round(confidence, 3),
                "reason": "low_confidence",
                "source": source,
            }
            print(
                f"[7] Skipped: low confidence "
                f"('{primary_term}' -> {confidence:.2f} < {CONFIDENCE_THRESHOLD})"
            )
            return result

        # ── Apply mapping ─────────────────────────────────────────────
        mapped_kpi_name = kpi_dict.get("name")

        result["kpi"] = mapped_kpi_name

        # If intent was UNKNOWN and we resolved a KPI, upgrade to SEGMENT_BY
        if intent_type == "UNKNOWN":
            result["intent"] = "SEGMENT_BY"
            print(f"[7] Upgraded UNKNOWN -> SEGMENT_BY")

        result["semantic_meta"] = {
            "applied": True,
            "input": query,
            "vague_term": primary_term,
            "mapped_to": mapped_kpi_name,
            "confidence": round(confidence, 3),
            "source": source,
        }

        print(
            f'[7] Semantic mapping: "{primary_term}" → "{mapped_kpi_name}" '
            f"({confidence:.2f} confidence)"
        )

        return result


# ─────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────


def create_semantic_interpreter(df, schema_mapper) -> SemanticInterpreter:
    """Factory function for SemanticInterpreter."""
    return SemanticInterpreter(df, schema_mapper)
