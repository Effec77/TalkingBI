import codecs
import os

# Phase 9B FINAL STABILIZATION

# 1. Update contracts.py with new trace fields
contracts_path = 'models/contracts.py'
if os.path.exists(contracts_path):
    print("Patching contracts.py...")
    contracts = codecs.open(contracts_path, 'r', 'utf-8').read()
    
    # Add new fields to ExecutionTrace
    new_trace_fields = '''    # Fixes
    kpi_validation: Dict[str, Any] = field(default_factory=dict)
    cache_used: bool = False
    cache_reason: str = ""
    context_applied: bool = False
    kpi_resolution: Dict[str, Any] = field(default_factory=dict)'''
    
    if 'context_applied' not in contracts:
        contracts = contracts.replace(
            '''    # Fixes
    kpi_validation: Dict[str, Any] = field(default_factory=dict)
    cache_used: bool = False
    cache_reason: str = ""''',
            new_trace_fields
        )
        
        # Update to_dict
        contracts = contracts.replace(
            '''            "kpi_validation": self.kpi_validation,
            "cache_used": self.cache_used,
            "cache_reason": self.cache_reason,
        }''',
            '''            "kpi_validation": self.kpi_validation,
            "cache_used": self.cache_used,
            "cache_reason": self.cache_reason,
            "context_applied": self.context_applied,
            "kpi_resolution": self.kpi_resolution,
        }'''
        )
        codecs.open(contracts_path, 'w', 'utf-8').write(contracts)

# 2. Update schema_mapper.py for Rule 1 (KPI Resolution Guarantee)
mapper_path = 'services/schema_mapper.py'
if os.path.exists(mapper_path):
    print("Patching schema_mapper.py...")
    mapper = codecs.open(mapper_path, 'r', 'utf-8').read()
    
    # Rule 1 implementation
    new_map_kpi = '''    def map_kpi(self, user_term: str) -> Tuple[Optional[str], str]:
        """
        Map user KPI term to schema KPI.
        
        Resolution order:
        1. Exact match (case-insensitive)
        2. Normalized match
        3. Schema map lookup
        4. Target column contains check (fuzzy fallback)
        5. Else -> None
        """
        if not user_term:
            return None, "no_term"

        user_lower = user_term.lower().strip()
        user_normalized = self.normalize(user_term)
        
        # Rule 1: STRICT - if it exists in columns, ALWAYS resolve
        col_lower_map = {c.lower(): c for c in self.columns}
        if user_lower in col_lower_map:
            return col_lower_map[user_lower], "exact_column_match"

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
                for col_norm, col_raw in {self.normalize(c): c for c in self.columns}.items():
                    if col_norm == canonical.lower() or col_norm in [a.lower() for a in aliases]:
                        return col_raw, "schema_map_column_fallback"
                        
                return canonical, "schema_map_default"

        # 4. Fallback Rule 1: fuzzy contain match
        matches = []
        for col in self.columns:
            if user_lower in col.lower():
                matches.append(col)
        
        if len(matches) == 1:
            return matches[0], "fuzzy_contain_match"
        elif len(matches) > 1:
            return None, "ambiguous"

        return None, "no_match"'''
    
    # Replace map_kpi block
    import re
    mapper = re.sub(r'def map_kpi\(self, user_term: str\) -> Tuple\[Optional\[str\], str\]:.*?return None, "no_match"', new_map_kpi, mapper, flags=re.DOTALL)
    codecs.open(mapper_path, 'w', 'utf-8').write(mapper)

# 3. Update context_resolver.py for Rule 3 (Context Carry)
resolver_path = 'services/context_resolver.py'
if os.path.exists(resolver_path):
    print("Patching context_resolver.py...")
    resolver = codecs.open(resolver_path, 'r', 'utf-8').read()
    
    # Ensure ResolutionResult has context_applied (it has context_used, but we'll add applied for Rule 3)
    if 'context_applied: bool = False' not in resolver:
        resolver = resolver.replace(
            'context_used: bool = False',
            'context_used: bool = False\n    context_applied: bool = False'
        )
    
    # Implement Rule 3 in resolve
    resolve_inject = '''        # Rule 3: Context carry for KPI
        context_applied = False
        if not kpi:
            last_context = self.get_last_resolved_context()
            if last_context and last_context.get("kpi"):
                kpi = last_context["kpi"]
                context_applied = True
                print(f"[6C] Context applied: KPI={kpi}")'''
    
    resolver = resolver.replace(
        'kpi = parsed_intent.get("kpi")',
        'kpi = parsed_intent.get("kpi")\n' + resolve_inject
    )
    
    # Update return to include context_applied
    resolver = resolver.replace(
        'context_used=context_used,',
        'context_used=context_used,\n                context_applied=context_applied,'
    )
    
    codecs.open(resolver_path, 'w', 'utf-8').write(resolver)

# 4. Update orchestrator.py for Rule 4 (Trend Detection) and Rule 2 (Invalid avoidance)
orch_path = 'services/orchestrator.py'
if os.path.exists(orch_path):
    print("Patching orchestrator.py...")
    orch = codecs.open(orch_path, 'r', 'utf-8').read()
    
    # Rule 4: Trend detection
    trend_logic = '''            # Rule 4: Trend detection BEFORE parser
            normalized_query_lower = normalized_query.lower()
            if "trend" in normalized_query_lower or "over time" in normalized_query_lower:
                # Simple rule-based detector
                date_col = None
                from services.dataset_profiler import profile_dataset
                profile = profile_dataset(df)
                if profile.datetime_columns:
                    date_col = profile.datetime_columns[0]
                
                if date_col:
                    print(f"[ORCHESTRATOR] 📈 Trend detected, using dimension: {date_col}")
                    if not deterministic_intent:
                         # Merge with intent
                         if 'intent' in locals() and intent:
                             intent["intent"] = "SEGMENT_BY"
                             intent["dimension"] = date_col
                         elif deterministic_intent:
                             deterministic_intent["intent"] = "SEGMENT_BY"
                             deterministic_intent["dimension"] = date_col'''
                             
    orch = orch.replace(
        'detector = DeterministicIntentDetector(schema_mapper, context_history)',
        'detector = DeterministicIntentDetector(schema_mapper, context_history)\n' + trend_logic
    )
    
    # Rule 2: NEVER INVALID, Rule 6 trace
    orch = orch.replace(
        'if not is_valid:',
        '''            # Rule 1/6 Trace
            trace.kpi_resolution = {
                "input": intent.get("kpi"),
                "resolved_to": intent.get("kpi") if is_valid else None,
                "columns": list(df.columns)
            }
            
            if not is_valid:'''
    )
    
    # Rule 2: Status INVALID -> AMBIGUOUS/UNKNOWN
    orch = orch.replace(
        'status="INVALID"',
        'status="AMBIGUOUS" if error_msg == "ambiguous" else "UNKNOWN"'
    )
    
    # Inject context_applied trace
    orch = orch.replace(
        'trace.context_used = resolution_result.context_used',
        'trace.context_used = resolution_result.context_used\n            trace.context_applied = getattr(resolution_result, "context_applied", False)'
    )

    codecs.open(orch_path, 'w', 'utf-8').write(orch)

print("Final Stabilization Patch Applied.")
