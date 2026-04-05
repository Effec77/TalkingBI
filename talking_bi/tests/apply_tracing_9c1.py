import codecs
import re
import os

# Phase 9C.1 Tracing Patch

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

# Orchestrator Tracing
patch_file('services/orchestrator.py',
'intent = semantic_interpreter.interpret(normalized_query, intent)',
'''print(f"[TRACE:ORCHESTRATOR] BEFORE SEMANTIC: {intent}")
            intent = semantic_interpreter.interpret(normalized_query, intent)
            print(f"[TRACE:ORCHESTRATOR] AFTER SEMANTIC: {intent}")''')

patch_file('services/orchestrator.py',
'intent = schema_mapper.map_intent(intent)',
'''print(f"[TRACE:ORCHESTRATOR] BEFORE SCHEMA: {intent}")
            intent = schema_mapper.map_intent(intent)
            print(f"[TRACE:ORCHESTRATOR] AFTER SCHEMA: {intent}")''')

patch_file('services/orchestrator.py',
'resolution_result = resolver.resolve(intent, dashboard_plan_dict, current_columns=list(df.columns))',
'''print(f"[TRACE:ORCHESTRATOR] BEFORE RESOLVER: {intent}")
            resolution_result = resolver.resolve(intent, dashboard_plan_dict, current_columns=list(df.columns))
            print(f"[TRACE:ORCHESTRATOR] AFTER RESOLVER: {resolution_result.intent if resolution_result else 'None'}")''')

# Semantic Interpreter Tracing
patch_file('services/semantic_interpreter.py',
'def interpret(self, query: str, intent: Dict[str, Any]) -> Dict[str, Any]:',
'''def interpret(self, query: str, intent: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE:SEMANTIC] Input: {intent}")
        if intent.get("_locked"):
            print(f"[TRACE:SEMANTIC] LOCK ACTIVE: {intent.get('_lock_source')}")
''')

# Schema Mapper Tracing
patch_file('services/schema_mapper.py',
'def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:',
'''def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE:SCHEMA] Input: {intent_dict}")
        if intent_dict.get("_locked"):
            print(f"[TRACE:SCHEMA] LOCK ACTIVE: {intent_dict.get('_lock_source')}")
''')

# Context Resolver Tracing
patch_file('services/context_resolver.py',
'def resolve(',
'''def resolve(
        self,
        parsed_intent: Dict[str, Any],
        dashboard_plan: Optional[Dict[str, Any]] = None,
        current_columns: List[str] = None,
    ) -> ResolutionResult:
        print(f"[TRACE:RESOLVER] Input: {parsed_intent}")
        if parsed_intent.get("_locked"):
            print(f"[TRACE:RESOLVER] LOCK ACTIVE: {parsed_intent.get('_lock_source')}")
''')

# Fix _resolve_compare trace
patch_file('services/context_resolver.py',
'def _resolve_compare(',
'''def _resolve_compare(
        self, parsed_intent: Dict[str, Any], dashboard_plan: Optional[Dict[str, Any]]
    ) -> ResolutionResult:
        print(f"[TRACE:RESOLVER:COMPARE] Input: {parsed_intent}")
''')

print("Tracing patches applied.")
