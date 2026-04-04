import codecs
import os
import re

# FINAL 1% - TREND DETECTION SYSTEM

def patch_contracts():
    contracts_path = 'models/contracts.py'
    if os.path.exists(contracts_path):
        print("Patching contracts.py for trend tracing...")
        content = codecs.open(contracts_path, 'r', 'utf-8').read()
        
        # Add trend tracing fields if missing
        if 'trend_detected' not in content:
            content = content.replace(
                'kpi_resolution: Dict[str, Any] = field(default_factory=dict)',
                'kpi_resolution: Dict[str, Any] = field(default_factory=dict)\n    trend_detected: bool = False\n    trend_dimension: Optional[str] = None'
            )
            
            content = content.replace(
                '"kpi_resolution": self.kpi_resolution,',
                '"kpi_resolution": self.kpi_resolution,\n            "trend_detected": self.trend_detected,\n            "trend_dimension": self.trend_dimension,'
            )
            codecs.open(contracts_path, 'w', 'utf-8').write(content)

def patch_orchestrator():
    orch_path = 'services/orchestrator.py'
    if os.path.exists(orch_path):
        print("Patching orchestrator.py for robust trend detection...")
        content = codecs.open(orch_path, 'r', 'utf-8').read()
        
        # NEW ROBUST TREND DETECTION LOGIC
        new_trend_block = '''            # Rule 1/2/3/4/6: Robust Trend Detection
            query_lower = normalized_query.lower()
            force_trend = any(k in query_lower for k in ["trend", "trends", "over time"])
            
            detected_date_col = None
            if force_trend:
                # Priority 1: Profile-based datetime detection
                from services.dataset_profiler import profile_dataset
                profile = profile_dataset(df)
                if profile.datetime_columns:
                    detected_date_col = profile.datetime_columns[0]
                
                # Priority 2: Name-based fallback
                if not detected_date_col:
                    for col in df.columns:
                        if any(k in col.lower() for k in ["date", "time", "month", "year"]):
                            detected_date_col = col
                            break
                
                if detected_date_col:
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
                        deterministic_intent = trend_intent
                else:
                    # Rule 4: Fail Safe
                    print("[ORCHESTRATOR] ⚠️ Trend requested but no date column found")
                    return OrchestratorResult(
                        status="INCOMPLETE",
                        query=query,
                        session_id=session_id,
                        intent={"intent": "SEGMENT_BY", "kpi": None, "dimension": None},
                        semantic_meta={},
                        data=[],
                        charts=[],
                        warnings=["No time dimension found for trend analysis"],
                        trace=trace.to_dict(),
                        latency_ms=(time.time() - start_time) * 1000
                    )
            # End Trend Logic'''

        # We replace the previous faulty trend block
        # The previous block started around 'normalized_query_lower = normalized_query.lower()' 
        # and ended before 'if deterministic_intent:'
        
        pattern = r'# Rule 4: Trend detection.*?# End Trend Logic' # If comments were exactly as I wrote 
        # But looking at previous view_file, it was different.
        
        old_pattern = r'# Rule 4: Trend detection\s+deterministic_intent = detector\.detect\(normalized_query\).*?deterministic_intent = trend_init'
        
        # Let's find a more stable anchor. 
        # We want to replace from 'deterministic_intent = detector.detect(normalized_query)' up to 
        # the closing brace/if for the trend check.
        
        # In current state of orchestrator.py viewed:
        # 134: deterministic_intent = detector.detect(normalized_query)
        # 135: 
        # 136: normalized_query_lower = normalized_query.lower()
        # ...
        # 158: deterministic_intent = trend_init
        
        match = re.search(r'deterministic_intent = detector\.detect\(normalized_query\)\s+normalized_query_lower = normalized_query\.lower\(\).*?deterministic_intent = trend_init', content, flags=re.DOTALL)
        if match:
            # We need to keep detector.detect() but move it or integrate.
            updated_chunk = 'deterministic_intent = detector.detect(normalized_query)\n' + new_trend_block
            content = content.replace(match.group(0), updated_chunk)
            codecs.open(orch_path, 'w', 'utf-8').write(content)
        else:
            print("Could not find exact trend block match, attempting fallback patch...")
            # Fallback for manually finding the block
            content = content.replace(
                'deterministic_intent = detector.detect(normalized_query)',
                'deterministic_intent = detector.detect(normalized_query)\n' + new_trend_block
            )
            codecs.open(orch_path, 'w', 'utf-8').write(content)

if __name__ == "__main__":
    patch_contracts()
    patch_orchestrator()
    print("Trend Fix Patch Applied.")
