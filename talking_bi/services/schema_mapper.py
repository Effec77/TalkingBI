"""
Phase 6F: Schema Intelligence Layer

Bridges user language with dataset schema WITHOUT semantic guessing.
Deterministic mapping only.

Resolution order:
1. Exact match
2. Normalized match
3. Schema map lookup
4. Fuzzy match (difflib, cutoff=0.8)
5. Else → None
"""

import re
import difflib
from typing import Dict, List, Optional, Any, Tuple


class SchemaMapper:
    """
    Maps user terminology to actual dataset schema elements.

    Responsibility:
    - Normalize user terms to match schema
    - Handle synonyms via static + auto-generated maps
    - Detect binary columns as valid KPIs
    - Provide transparent mapping metadata

    Rules:
    - NO LLM usage
    - NO semantic inference
    - Deterministic only
    """

    # Static synonym mappings (user term → canonical schema term)
    STATIC_SCHEMA_MAP = {
        # Revenue synonyms
        "revenue": [
            "sales",
            "amount",
            "total_amount",
            "total_revenue",
            "income",
            "turnover",
            "earnings",
        ],
        # Quantity synonyms
        "quantity": ["units", "volume", "count", "qty"],
        # Amount/Expense synonyms
        "amount": ["expenses", "cost", "spend", "spending", "total_amount"],
        # Churn synonyms
        "churn_flag": ["churn", "churned", "attrition", "cancelled", "canceled"],
        # Discount synonyms
        "discount": ["discounts", "savings", "deductions"],
        # Region synonyms
        "region": ["area", "territory", "zone", "location"],
        # Category synonyms (handles both "product_category" and "category")
        "product_category": [
            "category",
            "product_category",
            "product category",
            "type",
            "group",
        ],
        # Standalone category for finance dataset
        "category": [
            "category",
            "type",
            "group",
            "class",
        ],
        # Country synonyms
        "country": ["nation", "territory", "market"],
        # Plan synonyms (for SaaS)
        "subscription_plan": ["plan", "tier", "package", "subscription"],
    }

    def __init__(self, df, kpi_candidates: List[Dict]):
        """
        Initialize SchemaMapper with dataset.

        Args:
            df: DataFrame with dataset
            kpi_candidates: List of KPI candidate dicts from 0B
        """
        self.df = df
        self.kpi_candidates = kpi_candidates

        # Extract column names
        self.columns = list(df.columns)

        # Detect binary columns
        self.binary_columns = self.detect_binary_kpis(df)

        # Build comprehensive schema map
        self.schema_map = self.build_schema_map(df, kpi_candidates)

        # Build reverse lookup for KPIs
        self.kpi_names = [k.get("name", "").lower() for k in kpi_candidates]

        # Add binary columns as valid KPIs
        for col in self.binary_columns:
            if col.lower() not in self.kpi_names:
                self.kpi_names.append(col.lower())

    def normalize(self, text: str) -> str:
        """
        Normalize text for matching.

        Rules:
        - lowercase
        - replace spaces with underscores
        - remove special characters
        """
        if not text:
            return ""
        text = text.lower().strip()
        text = text.replace(" ", "_")
        text = text.replace("-", "_")
        text = re.sub(r"[^a-z0-9_]", "", text)
        return text

    def detect_binary_kpis(self, df) -> List[str]:
        """
        Identify binary columns (nunique == 2).

        Binary columns like churn_flag are valid KPIs.
        Returns list of binary column names.
        """
        binary_cols = []

        for col in df.columns:
            try:
                # Skip non-numeric columns for binary detection
                if df[col].dtype not in ["int64", "float64", "bool"]:
                    continue

                # Get unique non-null values
                unique_vals = df[col].dropna().unique()

                # Check if binary (2 unique values)
                if len(unique_vals) == 2:
                    # Check if values are 0/1 or True/False
                    vals = set(unique_vals)
                    if vals.issubset({0, 1, 0.0, 1.0, True, False}):
                        binary_cols.append(col)

            except Exception:
                continue

        return binary_cols

    def build_schema_map(self, df, kpi_candidates: List[Dict]) -> Dict[str, List[str]]:
        """
        Build comprehensive schema mapping.

        Combines:
        1. Static mappings (synonyms)
        2. Auto-generated from column names
        3. KPI candidate names
        """
        schema_map = {}

        # Start with static map
        for canonical, aliases in self.STATIC_SCHEMA_MAP.items():
            schema_map[canonical] = aliases.copy()

        # Add column name variations
        for col in self.columns:
            normalized = self.normalize(col)

            # Map normalized form to original
            if normalized not in schema_map:
                schema_map[normalized] = []
            if col not in schema_map[normalized]:
                schema_map[normalized].append(col)

            # Add space-separated variations
            space_form = col.replace("_", " ")
            if space_form != col and space_form not in schema_map.get(normalized, []):
                if normalized not in schema_map:
                    schema_map[normalized] = []
                schema_map[normalized].append(space_form)

        # Add KPI candidate aliases
        for kpi in kpi_candidates:
            name = kpi.get("name", "")
            source = kpi.get("source_column", "")

            if name:
                normalized_name = self.normalize(name)
                if normalized_name not in schema_map:
                    schema_map[normalized_name] = []

                # Add source column as alias
                if source and source != name:
                    if source not in schema_map[normalized_name]:
                        schema_map[normalized_name].append(source)

                # Add normalized source
                if source:
                    norm_source = self.normalize(source)
                    if norm_source not in schema_map[normalized_name]:
                        schema_map[normalized_name].append(norm_source)

        # Add binary columns with rate suffixes
        for col in self.binary_columns:
            normalized = self.normalize(col)

            # Map churn_flag → churn_rate
            if normalized not in schema_map:
                schema_map[normalized] = []

            # Add without _flag suffix
            if "_flag" in normalized:
                base = normalized.replace("_flag", "")
                if base not in schema_map[normalized]:
                    schema_map[normalized].append(base)
                if f"{base}_rate" not in schema_map[normalized]:
                    schema_map[normalized].append(f"{base}_rate")

        return schema_map

    def fuzzy_match(self, term: str, candidates: List[str]) -> Optional[str]:
        """
        Use difflib for fuzzy string matching.

        Args:
            term: User's term
            candidates: List of valid schema terms

        Returns:
            Best match or None (cutoff=0.8)
        """
        if not term or not candidates:
            return None

        matches = difflib.get_close_matches(
            term.lower(), [c.lower() for c in candidates], n=1, cutoff=0.8
        )

        if matches:
            # Find original case version
            match_lower = matches[0]
            for candidate in candidates:
                if candidate.lower() == match_lower:
                    return candidate
            return match_lower

        return None

    def map_kpi(self, user_term: str) -> Tuple[Optional[str], str]:
        """
        Map user KPI term to schema KPI.

        Resolution order:
        1. Exact match
        2. Normalized match
        3. Schema map lookup
        4. Fuzzy match
        5. Else → None

        Returns:
            (mapped_value, source)
        """
        if not user_term:
            return None, "no_term"

        user_lower = user_term.lower()
        user_normalized = self.normalize(user_term)

        # 1. Exact match against KPI candidates
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if name.lower() == user_lower:
                return name, "exact_match"

        # 2. Normalized match
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if self.normalize(name) == user_normalized:
                return name, "normalized_match"

        # 3. Schema map lookup
        for canonical, aliases in self.schema_map.items():
            # Check if user term matches canonical or any alias
            if user_lower == canonical.lower():
                # Find actual KPI name that matches this canonical
                for kpi in self.kpi_candidates:
                    kpi_name = kpi.get("name", "").lower()
                    kpi_source = kpi.get("source_column", "").lower()
                    if kpi_name == canonical.lower() or kpi_source == canonical.lower():
                        return kpi.get("name"), "schema_map"
                return canonical, "schema_map"

            # Check aliases
            for alias in aliases:
                if user_lower == alias.lower():
                    # Return canonical form
                    for kpi in self.kpi_candidates:
                        kpi_name = kpi.get("name", "").lower()
                        kpi_source = kpi.get("source_column", "").lower()
                        if (
                            kpi_name == canonical.lower()
                            or kpi_source == canonical.lower()
                        ):
                            return kpi.get("name"), "schema_map"
                    return canonical, "schema_map"

        # 4. Fuzzy match against KPI names (REJECTED - low confidence)
        # Disabled to prevent over-permissive mapping
        # Only exact, normalized, and schema_map matches are accepted
        kpi_names = [k.get("name", "") for k in self.kpi_candidates]
        fuzzy_result = self.fuzzy_match(user_term, kpi_names)
        if fuzzy_result:
            print(
                f"[6F] Mapping REJECTED: '{user_term}' -> '{fuzzy_result}' (fuzzy_match - low confidence)"
            )

        # Also check fuzzy match on binary columns for logging
        fuzzy_binary = self.fuzzy_match(user_term, self.binary_columns)
        if fuzzy_binary and not fuzzy_result:
            print(
                f"[6F] Mapping REJECTED: '{user_term}' -> '{fuzzy_binary}' (fuzzy_match on binary - low confidence)"
            )

        # 5. No match
        return None, "no_match"

    def map_dimension(self, user_term: str) -> Tuple[Optional[str], str]:
        """
        Map user dimension term to schema column.

        Same logic as map_kpi but against all columns.
        """
        if not user_term:
            return None, "no_term"

        user_lower = user_term.lower()
        user_normalized = self.normalize(user_term)

        # 1. Exact match
        for col in self.columns:
            if col.lower() == user_lower:
                return col, "exact_match"

        # 2. Normalized match
        for col in self.columns:
            if self.normalize(col) == user_normalized:
                return col, "normalized_match"

        # 3. Schema map lookup (reverse)
        for canonical, aliases in self.schema_map.items():
            if user_lower == canonical.lower():
                # Find actual column
                for col in self.columns:
                    if col.lower() == canonical.lower():
                        return col, "schema_map"
            for alias in aliases:
                if user_lower == alias.lower():
                    for col in self.columns:
                        if self.normalize(col) == canonical:
                            return col, "schema_map"

        # 4. Fuzzy match (REJECTED - low confidence)
        # Disabled to prevent over-permissive mapping
        fuzzy_result = self.fuzzy_match(user_term, self.columns)
        if fuzzy_result:
            print(
                f"[6F] Mapping REJECTED: '{user_term}' -> '{fuzzy_result}' (fuzzy_match - low confidence)"
            )

        # 5. No match
        return None, "no_match"

    def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map all schema-relevant fields in intent.

        Input:
            {"kpi": "...", "dimension": "...", "filter": "..."}

        Output:
            {
                "kpi": mapped_value,
                "dimension": mapped_value,
                "filter": mapped_value,
                "mapping_meta": {
                    "kpi_source": "...",
                    "dimension_source": "...",
                    "confidence": "high/medium/low"
                }
            }
        """
        result = intent_dict.copy()
        mapping_meta = {
            "kpi_source": None,
            "dimension_source": None,
            "filter_source": None,
            "confidence": "high",
        }

        # Map KPI
        if intent_dict.get("kpi"):
            mapped_kpi, source = self.map_kpi(intent_dict["kpi"])
            if mapped_kpi:
                result["kpi"] = mapped_kpi
                mapping_meta["kpi_source"] = source
                print(
                    f"[6F] Mapping: '{intent_dict['kpi']}' -> '{mapped_kpi}' ({source})"
                )
            else:
                mapping_meta["kpi_source"] = "unmapped"
                mapping_meta["confidence"] = "low"

        # Map dimension
        if intent_dict.get("dimension"):
            mapped_dim, source = self.map_dimension(intent_dict["dimension"])
            if mapped_dim:
                result["dimension"] = mapped_dim
                mapping_meta["dimension_source"] = source
                print(
                    f"[6F] Mapping: '{intent_dict['dimension']}' -> '{mapped_dim}' ({source})"
                )
            else:
                mapping_meta["dimension_source"] = "unmapped"
                mapping_meta["confidence"] = "low"

        # Map filter (treat as dimension value)
        if intent_dict.get("filter"):
            # Filters are values, not column names - keep as-is
            mapping_meta["filter_source"] = "user_value"

        # Determine overall confidence
        sources = [mapping_meta["kpi_source"], mapping_meta["dimension_source"]]
        if any(s in ["fuzzy_match", "no_match", "unmapped"] for s in sources if s):
            mapping_meta["confidence"] = "medium"
        if all(s in ["exact_match", "normalized_match"] for s in sources if s):
            mapping_meta["confidence"] = "high"

        result["mapping_meta"] = mapping_meta
        return result


def create_schema_mapper(df, kpi_candidates: List[Dict]) -> SchemaMapper:
    """Factory function for SchemaMapper."""
    return SchemaMapper(df, kpi_candidates)
