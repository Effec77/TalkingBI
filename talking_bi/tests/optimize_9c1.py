import codecs
import re
import os

# Phase 9C.1 Final Optimization & Cleanup

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
        # Try regex if exact match fails (for tracing variants)
        new_content = re.sub(re.escape(old_text).replace(r'\ ', r'\s+'), new_text, content)
        if new_content != content:
            codecs.open(path, 'w', 'utf-8').write(new_content)
            print(f"Patched {path} (via regex)")
        else:
            print(f"Could not find target text in {path}")

# 1. Fix Orchestrator Order (Move filter interpretation AFTER schema_mapper call)
# Actually, it's better to bypass confidence check for locked/filter-noun intents.

patch_file('services/orchestrator.py',
'''            # Rule 8: If overall confidence is low, mark as INCOMPLETE/UNKNOWN
            if overall_conf < 0.5 and intent.get("intent") != "UNKNOWN":''',
'''            # Rule 8: If overall confidence is low, mark as INCOMPLETE/UNKNOWN
            # Phase 9C.1 Bypass for locked or special intents
            if overall_conf < 0.5 and intent.get("intent") != "UNKNOWN" and not intent.get("_locked") and not trace.filter_interpretation:''')

# 2. Fix Context Resolver Null variable 'context_applied' in _resolve_compare
patch_file('services/context_resolver.py',
'''        missing_fields = []

        # Resolve kpi_1 (USER > CONTEXT)''',
'''        missing_fields = []
        context_applied = False

        # Resolve kpi_1 (USER > CONTEXT)''')

patch_file('services/context_resolver.py',
'''                kpi_1_source = "context"
                source_map["kpi_1"] = "context"''',
'''                kpi_1_source = "context"
                source_map["kpi_1"] = "context"
                context_applied = True''')

# 3. Cleanup Tracing (Optional but good for production)
# Let's keep tracing for now but I'll remove it in the final summary if needed.
# For now, let's just make the engine WORK.

print("Optimization patches applied.")
