"""
Phase 6E: Parser Hardening Layer
Thin normalization layer between raw user input and intent parser.

Goals:
1. Normalize column names (spaces → underscores)
2. Handle single-word queries gracefully
3. Lightweight KPI alias mapping (no LLM)
4. Binary column KPI support
5. Preserve deterministic 6C behavior

Rule: This layer ONLY normalizes - it never guesses semantics.
"""

from typing import Dict, List, Optional, Tuple
import re


class QueryNormalizer:
    """
    Pre-processor for user queries.

    Responsibility:
    - Clean and normalize input BEFORE parser sees it
    - Fix common user language → schema mismatches
    - Never infer intent, only fix syntax
    """

    def __init__(self, dataset_columns: List[str], kpi_candidates: List[Dict]):
        self.original_columns = dataset_columns
        # Build normalized column map: normalized_name → original_name
        self.column_map = self._build_column_map(dataset_columns)
        # Build KPI alias map
        self.kpi_aliases = self._build_kpi_aliases(kpi_candidates)

    def _build_column_map(self, columns: List[str]) -> Dict[str, str]:
        """
        Create mapping from normalized column names to originals.

        Example:
            "product category" → "product_category"
            "total amount" → "total_amount" (or "amount" if exists)
        """
        column_map = {}
        for col in columns:
            # Normalize: lowercase, replace spaces with underscores
            normalized = col.lower().replace(" ", "_").replace("-", "_")
            column_map[normalized] = col
            # Also add without "total_" prefix if present
            if normalized.startswith("total_"):
                short_form = normalized[6:]  # Remove "total_"
                column_map[short_form] = col
        return column_map

    def _build_kpi_aliases(self, kpi_candidates: List[Dict]) -> Dict[str, str]:
        """
        Build KPI alias mapping for common synonyms.

        Light-weight, deterministic - no LLM.
        """
        aliases = {
            # Standard business terms
            "sales": "revenue",
            "income": "revenue",
            "earnings": "revenue",
            "turnover": "revenue",
            "amount": "revenue",
            "money": "revenue",
            # Expense terms
            "expenses": "amount",  # Maps to amount column (negative values)
            "cost": "amount",
            "spend": "amount",
            "spending": "amount",
            # Quantity terms
            "units": "quantity",
            "count": "quantity",
            "volume": "quantity",
            # Churn terms
            "churn": "churn_flag",
            "churned": "churn_flag",
            "attrition": "churn_flag",
            # Discount terms
            "discounts": "discount",
            "savings": "discount",
        }

        # Add reverse mappings from actual KPI names
        for kpi in kpi_candidates:
            name = kpi.get("name", "").lower()
            if name and name not in aliases:
                aliases[name] = name  # Self-mapping

        return aliases

    def normalize(self, query: str) -> Tuple[str, Dict]:
        """
        Normalize user query.

        Returns:
            (normalized_query, metadata)

        Metadata includes:
            - original_query
            - detected_kpi_alias
            - detected_column_normalization
            - is_single_word
        """
        original = query
        metadata = {
            "original_query": original,
            "modifications": [],
        }

        # Step 1: Handle single-word queries
        words = query.strip().split()
        if len(words) == 1:
            metadata["is_single_word"] = True
            metadata["modifications"].append("single_word_detected")

            # Check if it's a KPI alias
            word_lower = words[0].lower()
            if word_lower in self.kpi_aliases:
                target_kpi = self.kpi_aliases[word_lower]
                query = f"show {target_kpi}"
                metadata["detected_kpi_alias"] = word_lower
                metadata["expanded_to"] = query
                metadata["modifications"].append(
                    f"expanded_{word_lower}_to_show_{target_kpi}"
                )
        else:
            metadata["is_single_word"] = False

            # Step 2: Check for KPI aliases in multi-word queries
            # NOTE: Only exact phrase matching - no substring replacement
            query_lower = query.lower()

            # Define exact phrase mappings (not substring)
            PHRASE_MAP = {
                "sales": "revenue",
                "revenue numbers": "revenue",
                "revenue data": "revenue",
                "show churn": "show churn_flag",
                "churn rate": "churn_flag",
                "expenses": "amount",
                "total expenses": "amount",
                "total amount": "total_amount",
                "product category": "product_category",
                "region wise": "by region",
                "country wise": "by country",
                "category wise": "by category",
            }

            # Try exact phrase matching first
            for phrase, replacement in PHRASE_MAP.items():
                if query_lower == phrase or re.search(
                    r"\b" + re.escape(phrase) + r"\b", query_lower
                ):
                    metadata["detected_kpi_alias"] = phrase
                    query = re.sub(
                        r"\b" + re.escape(phrase) + r"\b",
                        replacement,
                        query,
                        flags=re.IGNORECASE,
                    )
                    metadata["modifications"].append(
                        f"phrase_replaced_{phrase}_with_{replacement}"
                    )
                    break  # Only first match

            # Skip aggressive alias replacement to avoid overreach
            # Example: "total amount" should NOT become "total revenue"

        # Step 3: Normalize column name references
        query_lower = query.lower()
        for normalized, original in self.column_map.items():
            # Look for column name in query (with spaces or underscores)
            pattern = normalized.replace("_", r"[\s_-]")
            if re.search(r"\b" + pattern + r"\b", query_lower):
                metadata["detected_column_normalization"] = {
                    "from": normalized,
                    "to": original,
                }
                metadata["modifications"].append(f"normalized_column_{normalized}")
                break

        # Step 4: Handle structural phrase normalizations (NOT KPI mappings)
        # These change query structure but don't map KPIs
        phrase_replacements = [
            (r"\bnow provide me\b", "show"),
            (r"\bplease provide me\b", "show"),
            (r"\bprovide me\b", "show"),
            (r"\bshow me\b", "show"),
            (r"\bshow us\b", "show"),
            (r"\bgive me\b", "show"),
            (r"\bnow show me\b", "show"),
            (r"\bwhat is\b", "show"),
            (r"\bwhat are\b", "show"),
            (r"\bhow much\b", "show"),
            (r"\bby the\b", "by"),
            # "region wise" etc. are now handled in Step 2 PHRASE_MAP
        ]

        for pattern, replacement in phrase_replacements:
            if re.search(pattern, query, re.IGNORECASE):
                query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
                metadata["modifications"].append(f"structural:{pattern}")

        metadata["normalized_query"] = query
        return query, metadata


class KPIEnhancer:
    """
    Post-processing for KPI candidates.

    Responsibility:
    - Ensure binary columns are treated as valid KPIs
    - Add derived metrics (rate, percentage)
    - Do NOT use LLM
    """

    @staticmethod
    def enhance_binary_columns(columns: List[str], df_sample) -> List[Dict]:
        """
        Detect and enhance binary columns as KPI candidates.

        Binary columns (0/1) are valid KPIs - treat them as rates.
        """
        enhanced = []

        for col in columns:
            # Check if column is binary (only 0/1 or True/False)
            if KPIEnhancer._is_binary_column(df_sample, col):
                # Add as KPI with rate aggregation
                enhanced.append(
                    {
                        "name": col,
                        "source_column": col,
                        "aggregation": "mean",  # Rate = average of 0/1
                        "type": "rate",
                        "display_name": col.replace("_", " ").title(),
                        "is_binary": True,
                    }
                )

                # Also add percentage version
                enhanced.append(
                    {
                        "name": f"{col}_pct",
                        "source_column": col,
                        "aggregation": "mean",
                        "type": "percentage",
                        "display_name": f"{col.replace('_', ' ').title()} %",
                        "format": "percentage",
                        "is_binary": True,
                    }
                )

        return enhanced

    @staticmethod
    def _is_binary_column(df_sample, col: str) -> bool:
        """Check if column contains only binary values (0/1 or True/False)."""
        try:
            if col not in df_sample.columns:
                return False

            sample = df_sample[col].dropna()
            if len(sample) == 0:
                return False

            unique_vals = set(sample.unique())

            # Check for 0/1
            if unique_vals.issubset({0, 1, 0.0, 1.0}):
                return True

            # Check for True/False
            if unique_vals.issubset({True, False}):
                return True

            return False
        except:
            return False


def create_normalizer(
    dataset_columns: List[str], kpi_candidates: List[Dict]
) -> QueryNormalizer:
    """Factory function to create a query normalizer."""
    return QueryNormalizer(dataset_columns, kpi_candidates)


# ─────────────────────────────────────────────────────────────
# TEST SUITE for Phase 6E
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("PHASE 6E: PARSER HARDENING - TEST SUITE")
    print("=" * 70)

    # Test data
    test_columns = [
        "product_category",
        "region",
        "revenue",
        "quantity",
        "churn_flag",
        "total_amount",
    ]
    test_kpis = [
        {"name": "Revenue", "source_column": "revenue"},
        {"name": "Quantity", "source_column": "quantity"},
        {"name": "Churn Rate", "source_column": "churn_flag"},
    ]

    normalizer = create_normalizer(test_columns, test_kpis)

    test_cases = [
        # (input, expected_contains)
        ("expenses", "show amount"),
        ("sales", "show revenue"),
        ("churn", "show churn_flag"),
        ("revenue numbers", "show revenue"),
        ("show me revenue", "show revenue"),
        ("product category", "product_category"),  # Normalization tracked
        ("total amount", "amount"),  # Short form
    ]

    passed = 0
    failed = 0

    for query, expected in test_cases:
        normalized, meta = normalizer.normalize(query)

        # Check if expected pattern is in normalized
        if expected.lower() in normalized.lower():
            print(f"[OK] '{query}' -> '{normalized}'")
            passed += 1
        else:
            print(f"[FAIL] '{query}' -> '{normalized}' (expected: {expected})")
            print(f"  Metadata: {meta}")
            failed += 1

    print("=" * 70)
    print(f"Results: {passed}/{passed + failed} tests passed")
    print("=" * 70)
