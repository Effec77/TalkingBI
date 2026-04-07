import codecs
import re
import os

# Phase 9C.1 Final Fixes Patch

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

# FIX 1: Trend Intent Lock (orchestrator.py)
patch_file('services/orchestrator.py',
'''                if detected_date_col:
                    print(f"[ORCHESTRATOR] 📈 Trend detected, using dimension: {detected_date_col}")
                    trace.trend_detected = True
                    trace.trend_dimension = detected_date_col
                    
                    # Rule 2: Force Intent
                    trend_intent = {
                        "intent": "SEGMENT_BY",
                        "dimension": detected_date_col,
                        "kpi": None,
                        "filter": None
                    }
                    
                    # Ensure deterministic_intent reflects this
                    if deterministic_intent:
                        deterministic_intent.update(trend_intent)
                    else:
                        deterministic_intent = trend_intent''',
'''                if detected_date_col:
                    print(f"[ORCHESTRATOR] 📈 Trend detected, using dimension: {detected_date_col}")
                    trace.trend_detected = True
                    trace.trend_dimension = detected_date_col
                    trace.trend_locked = True
                    
                    # Rule 2: Force Intent & LOCK (Phase 9C.1)
                    trend_intent = {
                        "intent": "SEGMENT_BY",
                        "dimension": detected_date_col,
                        "kpi": None,
                        "filter": None,
                        "_locked": True,
                        "_lock_source": "trend"
                    }
                    
                    # Ensure deterministic_intent reflects this
                    if deterministic_intent:
                        deterministic_intent.update(trend_intent)
                    else:
                        deterministic_intent = trend_intent''')

# Fix 1 Downstream Lock Check (semantic_interpreter.py, schema_mapper.py, context_resolver.py)
patch_file('services/semantic_interpreter.py',
'''    def interpret(self, query: str, intent: Dict[str, Any]) -> Dict[str, Any]:''',
'''    def interpret(self, query: str, intent: Dict[str, Any]) -> Dict[str, Any]:
        # Fix 1: Trend Lock Check (Phase 9C.1)
        if intent.get("_locked"):
            return intent.copy()
''')

patch_file('services/schema_mapper.py',
'''    def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:''',
'''    def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:
        # Fix 1: Trend Lock Check (Phase 9C.1)
        if intent_dict.get("_locked"):
            return intent_dict.copy()
''')

patch_file('services/context_resolver.py',
'''    def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
        current_columns: List[str] = None,
    ) -> ResolutionResult:''',
'''    def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
        current_columns: List[str] = None,
    ) -> ResolutionResult:
        # Fix 1: Trend Lock Check (Phase 9C.1)
        if parsed_intent.get("_locked"):
            self.add_to_context(parsed_intent)
            return ResolutionResult(
                status=ResolutionStatus.RESOLVED.value,
                intent=parsed_intent.copy(),
                source_map={},
                warnings=[],
                missing_fields=[],
                context_used=False,
                context_applied=False,
            )
''')

# FIX 2: Filter Noun Interpretation (orchestrator.py)
filter_noun_fix = '''
            # FIX 2: Filter Noun Interpretation (Phase 9C.1)
            if normalized_query.lower().startswith("filter"):
                tokens = normalized_query.split()
                if len(tokens) == 2:
                    column_term = tokens[1]
                    # Map to actual column
                    mapped_col, _ = schema_mapper.map_dimension(column_term)
                    if mapped_col:
                        print(f"[ORCHESTRATOR] 🔍 Filter noun detected: {mapped_col}")
                        intent = {
                            "intent": "SUMMARIZE",
                            "kpi": None,
                            "dimension": None,
                            "filter": {
                                "column": mapped_col,
                                "operator": "NOT_NULL"
                            }
                        }
                        trace.filter_interpretation = "NOT_NULL"
                        trace.g6_applied = True
                        trace.g6_reason = "filter_noun"
                        # Execute early
                        deterministic_intent = intent
'''

patch_file('services/orchestrator.py',
'''            # Step 4: Deterministic override (6G)
            schema_mapper = create_schema_mapper(df, kpi_candidates)
            detector = DeterministicIntentDetector(schema_mapper, context_history)''',
'''            # Step 4: Deterministic override (6G)
            schema_mapper = create_schema_mapper(df, kpi_candidates)
            detector = DeterministicIntentDetector(schema_mapper, context_history)
''' + filter_noun_fix)

# FIX 2 Downstream Support (adaptive_executor.py)
patch_file('graph/adaptive_executor.py',
'''    # FIX 2: Handle null/none/nan values
    filter_str = str(filter_val).lower().strip()''',
'''    # FIX 2 Phase 9C.1: Handle structured NOT_NULL filter
    if isinstance(filter_val, dict) and filter_val.get("operator") == "NOT_NULL":
        col = filter_val.get("column")
        if col in df.columns:
            filtered = df[df[col].notna()]
            print(f"[6D:filter] Applied {col} IS NOT NULL → {len(filtered)} rows")
            return filtered
            
    # FIX 2: Handle null/none/nan values
    filter_str = str(filter_val).lower().strip()''')

# FIX 3: Compare Context Enforcement (context_resolver.py)
patch_file('services/context_resolver.py',
'''        # Resolve kpi_1 (USER > CONTEXT)
        resolved_kpi_1 = None
        kpi_1_source = None
        if kpi_1:
            # FIX 1: Normalize KPI
            resolved_kpi_1 = self._normalize_kpi(kpi_1)
            kpi_1_source = "user"
            source_map["kpi_1"] = "user"
        else:''' ,
'''        # Resolve kpi_1 (USER > CONTEXT) - Fix 3 Phase 9C.1
        resolved_kpi_1 = None
        kpi_1_source = None
        if kpi_1:
            # FIX 1: Normalize KPI
            resolved_kpi_1 = self._normalize_kpi(kpi_1)
            kpi_1_source = "user"
            source_map["kpi_1"] = "user"
        else:
            # Rule 3 Phase 9C.1: Compare context inheritance
            print(f"[6C] COMPARE intent missing kpi_1 - searching context")''' )

# Fix 3: Logic inside _resolve_compare is already somewhat there, but naming check is needed
# Re-patching _resolve_compare to be more strict as per rule
new_resolve_compare = '''    def _resolve_compare(
        self, parsed_intent: Dict[str, Any], dashboard_plan: Optional[Dict[str, Any]]
    ) -> ResolutionResult:
        """COMPARE intent resolution (Fix 3 Phase 9C.1)."""
        kpi_1 = parsed_intent.get("kpi_1") # mapped from 'kpi' in resolve()
        kpi_2 = parsed_intent.get("kpi_2")
        dimension = parsed_intent.get("dimension")
        filter_val = parsed_intent.get("filter")

        source_map = {}
        warnings: List[StructuredWarning] = []
        missing_fields = []
        context_applied = False

        # Resolve kpi_1 (USER > CONTEXT)
        resolved_kpi_1 = None
        if kpi_1:
            resolved_kpi_1 = self._normalize_kpi(kpi_1)
            source_map["kpi_1"] = "user"
        else:
            context = self.get_last_resolved_context()
            if context and context.get("kpi"):
                resolved_kpi_1 = self._normalize_kpi(context["kpi"])
                source_map["kpi_1"] = "context"
                context_applied = True
                warnings.append(StructuredWarning(type="context_inheritance", field="kpi_1", value=resolved_kpi_1, message=f"KPI-1 inherited from context: {resolved_kpi_1}"))
            else:
                missing_fields.append("kpi_1")

        # Resolve kpi_2 (USER ONLY)
        resolved_kpi_2 = None
        if kpi_2:
            resolved_kpi_2 = self._normalize_kpi(kpi_2)
            source_map["kpi_2"] = "user"
        else:
            missing_fields.append("kpi_2")

        if resolved_kpi_1 and resolved_kpi_2 and resolved_kpi_1.lower() == resolved_kpi_2.lower():
            missing_fields.append("kpi_2")

        # Resolve dimension (USER > CONTEXT)
        resolved_dimension = None
        if dimension:
            resolved_dimension = dimension
            source_map["dimension"] = "user"
        else:
            context = self.get_last_resolved_context()
            if context and context.get("dimension"):
                resolved_dimension = context["dimension"]
                source_map["dimension"] = "context"

        resolved_intent = {
            "intent": "COMPARE",
            "kpi_1": resolved_kpi_1,
            "kpi_2": resolved_kpi_2,
            "dimension": resolved_dimension,
            "filter": filter_val,
        }

        context_used = any(v == "context" for v in source_map.values())
        if missing_fields:
            return ResolutionResult(status=ResolutionStatus.INCOMPLETE.value, intent=resolved_intent, missing_fields=missing_fields, context_used=context_used, context_applied=context_applied)

        self.add_to_context(resolved_intent)
        return ResolutionResult(status=ResolutionStatus.RESOLVED.value, intent=resolved_intent, source_map=source_map, context_used=context_used, context_applied=context_applied)'''

path = 'services/context_resolver.py'
if os.path.exists(path):
    content = codecs.open(path, 'r', 'utf-8').read()
    content = re.sub(r'def _resolve_compare\(.*?return ResolutionResult\(status=ResolutionStatus\.RESOLVED\.value,.*?context_applied=context_applied,.*?\)', new_resolve_compare, content, flags=re.DOTALL)
    codecs.open(path, 'w', 'utf-8').write(content)
    print("Patched context_resolver.py (Fix 3)")

print("Phase 9C.1 final patches applied.")
