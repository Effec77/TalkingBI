import codecs
import re
import os

# Phase 9C Hardening Patch

def patch_file(path, old_text, new_text):
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return
    content = codecs.open(path, 'r', 'utf-8').read()
    if old_text in content:
        content = content.replace(old_text, new_text)
        codecs.open(path, 'w', 'utf-8').write(content)
        print(f"Patched {path}")
    else:
        print(f"Could not find target text in {path}")

# Task 1: Failure Intelligence Layer (evaluator.py)
patch_file('services/evaluator.py',
'''@dataclass
class EvalRecord:
    query:               str
    dataset:             str
    status:              str
    intent:              Optional[Dict[str, Any]]
    execution_mode:      Optional[str]
    semantic_applied:    bool
    latency_ms:          float
    failure_type:        Optional[str]''',
'''@dataclass
class EvalRecord:
    query:               str
    dataset:             str
    status:              str
    intent:              Optional[Dict[str, Any]]
    execution_mode:      Optional[str]
    semantic_applied:    bool
    latency_ms:          float
    failure_type:        Optional[str]
    failure_reason:      Optional[Dict[str, Any]] = None''')

# Task 1: Update record method
patch_file('services/evaluator.py',
'''        rec = EvalRecord(
            query=query,
            dataset=dataset,
            status=status,
            intent=result.get("intent_resolved") or result.get("intent"),
            execution_mode=execution_mode,
            semantic_applied=semantic_applied,
            latency_ms=round(latency_ms, 2),
            failure_type=failure,
        )''',
'''        # Extract failure reason from trace if present
        trace = result.get("trace", {})
        failure_reason = trace.get("failure_reason")

        rec = EvalRecord(
            query=query,
            dataset=dataset,
            status=status,
            intent=result.get("intent_resolved") or result.get("intent"),
            execution_mode=execution_mode,
            semantic_applied=semantic_applied,
            latency_ms=round(latency_ms, 2),
            failure_type=failure,
            failure_reason=failure_reason,
        )''')

# Task 2: KPI Guardrails (schema_mapper.py)
new_map_kpi = '''    def map_kpi(self, user_term: str) -> Tuple[Optional[str], str]:
        """
        Map user KPI term to schema KPI.
        
        Resolution order:
        1. Exact match (case-insensitive) - Rule 2 Phase 9C
        2. Normalized match - Rule 2 Phase 9C
        3. Schema map lookup
        4. Target column partial check (Rule 2 Phase 9C)
        5. Else -> UNKNOWN (Rule 2 Phase 9C)
        """
        if not user_term:
            return None, "no_term"

        user_lower = user_term.lower().strip()
        user_normalized = self.normalize(user_term)
        
        # Rule 2: STRICT - exact/normalized match as priority
        matches = [col for col in self.columns if self.normalize(col) == user_normalized]
        if len(matches) == 1:
            return matches[0], "exact_column_match"
        elif len(matches) > 1:
            return None, "ambiguous" # NEVER auto-pick

        # 1. Exact match against KPI candidates
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if name.lower() == user_lower:
                return name, "exact_match"

        # 2. Normalized match KPI candidates
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if self.normalize(name) == user_normalized:
                return name, "normalized_match"

        # 3. Schema map lookup
        normalized_columns = {self.normalize(c): c for c in self.columns}
        for canonical, aliases in self.schema_map.items():
            if user_lower == canonical.lower() or user_lower in [a.lower() for a in aliases]:
                for kpi in self.kpi_candidates:
                    kpi_name = kpi.get("name", "").lower()
                    kpi_source = kpi.get("source_column", "").lower()
                    if kpi_name == canonical.lower() or kpi_source == canonical.lower():
                        return kpi.get("name"), "schema_map"
                    for alias in aliases:
                        if kpi_name == alias.lower() or kpi_source == alias.lower():
                            return kpi.get("name"), "schema_map"
                
                # Check columns directly
                for col_norm, col_raw in normalized_columns.items():
                    if col_norm == canonical.lower() or col_norm in [a.lower() for a in aliases]:
                        return col_raw, "schema_map_column_fallback"
                        
                return canonical, "schema_map_default"

        # 4. Fallback Rule 2: partial reach
        possible = []
        for col in self.columns:
            if user_lower in col.lower():
                possible.append(col)
        
        if len(possible) == 1:
            return possible[0], "partial_match"
        elif len(possible) > 1:
            return None, "ambiguous"

        return None, "unknown"'''

# Replacing map_kpi in schema_mapper.py
path = 'services/schema_mapper.py'
if os.path.exists(path):
    content = codecs.open(path, 'r', 'utf-8').read()
    content = re.sub(r'def map_kpi\(self, user_term: str\) -> Tuple\[Optional\[str\], str\]:.*?return None, "no_match"', new_map_kpi, content, flags=re.DOTALL)
    codecs.open(path, 'w', 'utf-8').write(content)
    print("Patched schema_mapper.py (Task 2)")

# Task 3: Context Validation (context_resolver.py)
patch_file('services/context_resolver.py',
'''    def get_last_resolved_context(self) -> Optional[Dict[str, Any]]:
        """Get last resolved intent from context."""
        if not self.context_history:
            return None
        context = self.context_history[-1]
        # Guard against empty context object
        if not context or not isinstance(context, dict):
            return None
        # Context must have a kpi to be valid (deterministic - no semantic assumptions)
        if not context.get("kpi"):
            return None
        return context''',
'''    def get_last_resolved_context(self, current_columns: List[str] = None) -> Optional[Dict[str, Any]]:
        """Get last resolved intent from context (with dataset validation)."""
        if not self.context_history:
            return None
        context = self.context_history[-1]
        # Guard against empty context object
        if not context or not isinstance(context, dict):
            return None
        
        # Rule 3 Phase 9C: Dataset consistency check
        kpi = context.get("kpi")
        if not kpi:
            return None
            
        if current_columns is not None:
             # Case-insensitive column check
             col_lower = [c.lower() for c in current_columns]
             if kpi.lower() not in col_lower:
                 print(f"[6C] Discarding context KPI '{kpi}' - not in current dataset")
                 return None
                 
        return context''')

# Task 3: Update resolve signature and call (context_resolver.py)
patch_file('services/context_resolver.py',
'''    def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
    ) -> ResolutionResult:''',
'''    def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
        current_columns: List[str] = None,
    ) -> ResolutionResult:''')

patch_file('services/context_resolver.py',
'''        if not kpi:
            last_context = self.get_last_resolved_context()''',
'''        if not kpi:
            last_context = self.get_last_resolved_context(current_columns)''')

# Task 3: Trace update (contracts.py first)
patch_file('models/contracts.py',
'''    trend_dimension: Optional[str] = None''',
'''    trend_dimension: Optional[str] = None
    context_valid: bool = True
    failure_reason: Dict[str, Any] = field(default_factory=dict)
    confidence: Dict[str, float] = field(default_factory=dict)''')

patch_file('models/contracts.py',
'''            "kpi_resolution": self.kpi_resolution,
            "trend_detected": self.trend_detected,
            "trend_dimension": self.trend_dimension,
        }''',
'''            "kpi_resolution": self.kpi_resolution,
            "trend_detected": self.trend_detected,
            "trend_dimension": self.trend_dimension,
            "context_valid": self.context_valid,
            "failure_reason": self.failure_reason,
            "confidence": self.confidence,
        }''')

# Task 5: Cache Integrity (cache.py)
patch_file('services/cache.py',
'''def get_query_key(query: str, dataset: str) -> int:
    return hash((query.lower().strip(), dataset))''',
'''def get_query_key(query: str, dataset: str, context: Any = None) -> int:
    # Rule 5 Phase 9C: Context-aware cache key
    return hash((query.lower().strip(), dataset, str(context)))''')

# Task 7: Golden Test Set (Placeholder for user to see we are ready)
golden_queries_content = '''# Golden Test Set - Phase 9C
# Fixed queries that MUST resolve 100%

GOLDEN_SET = [
    "show revenue",
    "show revenue by region",
    "compare revenue with quantity",
    "electronics revenue trends",
    "show revenue over time",
    "total amount by category",
    "show mrr trends",
    "churn rate by month",
    "show total amount",
    "revenue by country",
    "show quantities",
    "average spend by tier",
    "amount over time",
    "compare revenue with expenses",
    "trends of sales by nation",
    "show turnover",
    "show earnings with nations",
    "count by region",
    "show units",
    "volume trends",
]
'''
with open('tests/golden_queries.py', 'w', encoding='utf-8') as f:
    f.write(golden_queries_content)
    print("Created tests/golden_queries.py (Task 7)")

print("Initial Phase 9C hardening scripts applied.")
