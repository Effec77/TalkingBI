"""
Test Multi-Provider LLM Orchestration - Phase 0B Patch
Tests the complete fallback chain and Python-first KPI selection
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from services.kpi_selector import select_kpis_python
from services.llm_manager import LLMManager
from services.kpi_enrichment import enrich_kpis
from services.intelligence_engine import generate_dashboard_plan
from models.contracts import UploadedDataset


def test_python_kpi_selection():
    """Test Python-first KPI selection (MUST work without LLM)"""
    print("\n" + "="*70)
    print("TEST 1: Python-First KPI Selection (No LLM Required)")
    print("="*70)
    
    # Create test dataset
    df = pd.DataFrame({
        'product': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
        'sales': [100, 200, 150, 300, 250, 180, 220, 190],
        'quantity': [10, 20, 15, 30, 25, 18, 22, 19],
        'profit': [50, 100, 75, 150, 125, 90, 110, 95],
        'region': ['North', 'South', 'East', 'West', 'North', 'South', 'East', 'West']
    })
    
    # Test Python selection
    kpis = select_kpis_python(df)
    
    # Assertions
    assert len(kpis) == 3, f"Expected 3 KPIs, got {len(kpis)}"
    assert all(col in df.columns for col in kpis), "All KPIs must be valid columns"
    
    print(f"✓ Python-first selection returned exactly 3 KPIs: {kpis}")
    print("✓ Test PASSED: System works without any LLM")


def test_llm_enrichment_with_fallback():
    """Test LLM enrichment with Python fallback"""
    print("\n" + "="*70)
    print("TEST 2: LLM Enrichment with Python Fallback")
    print("="*70)
    
    # Create test dataset
    df = pd.DataFrame({
        'sales': [100, 200, 150, 300, 250, 180, 220, 190],
        'quantity': [10, 20, 15, 30, 25, 18, 22, 19],
        'profit': [50, 100, 75, 150, 125, 90, 110, 95]
    })
    
    # Select KPIs
    kpi_columns = select_kpis_python(df)
    
    # Initialize LLM manager
    llm_manager = LLMManager()
    
    # Test enrichment
    dataset_context = {
        'filename': 'test_data.csv',
        'rows': len(df),
        'columns': list(df.columns)
    }
    
    enriched = enrich_kpis(kpi_columns, dataset_context, llm_manager)
    
    # Assertions
    assert len(enriched) == 3, f"Expected 3 enriched KPIs, got {len(enriched)}"
    assert all('name' in kpi for kpi in enriched), "All KPIs must have 'name'"
    assert all('source_column' in kpi for kpi in enriched), "All KPIs must have 'source_column'"
    assert all('aggregation' in kpi for kpi in enriched), "All KPIs must have 'aggregation'"
    
    print(f"✓ Enrichment returned 3 KPIs")
    for kpi in enriched:
        print(f"  - {kpi['name']} ({kpi['source_column']}, {kpi['aggregation']})")
    print("✓ Test PASSED: Enrichment works (with LLM or fallback)")


def test_complete_phase_0b_flow():
    """Test complete Phase 0B flow end-to-end"""
    print("\n" + "="*70)
    print("TEST 3: Complete Phase 0B Flow (End-to-End)")
    print("="*70)
    
    # Create test dataset
    df = pd.DataFrame({
        'product': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
        'sales': [100, 200, 150, 300, 250, 180, 220, 190],
        'quantity': [10, 20, 15, 30, 25, 18, 22, 19],
        'profit': [50, 100, 75, 150, 125, 90, 110, 95],
        'region': ['North', 'South', 'East', 'West', 'North', 'South', 'East', 'West']
    })
    
    # Create UploadedDataset
    uploaded_dataset = UploadedDataset(
        session_id="test-session",
        filename="test_data.csv",
        columns=list(df.columns),
        dtypes={col: str(df[col].dtype) for col in df.columns},
        shape=df.shape,
        sample_values={col: df[col].astype(str).unique()[:3].tolist() for col in df.columns},
        missing_pct={col: df[col].isna().mean() for col in df.columns}
    )
    
    # Run complete Phase 0B
    dashboard_plan = generate_dashboard_plan(
        session_id="test-session",
        df=df,
        uploaded_dataset=uploaded_dataset
    )
    
    # Assertions
    assert dashboard_plan is not None, "Dashboard plan must not be None"
    assert len(dashboard_plan.kpis) == 3, f"Expected 3 KPIs, got {len(dashboard_plan.kpis)}"
    assert len(dashboard_plan.charts) == 3, f"Expected 3 charts, got {len(dashboard_plan.charts)}"
    assert dashboard_plan.kpi_coverage == 1.0, f"Expected 100% coverage, got {dashboard_plan.kpi_coverage:.1%}"
    assert dashboard_plan.story_arc, "Story arc must not be empty"
    
    print(f"✓ Dashboard plan created successfully")
    print(f"  - KPIs: {len(dashboard_plan.kpis)}")
    print(f"  - Charts: {len(dashboard_plan.charts)}")
    print(f"  - Coverage: {dashboard_plan.kpi_coverage:.1%}")
    print("✓ Test PASSED: Complete Phase 0B flow works")


def test_zero_api_keys_mode():
    """Test system works with ZERO API keys (pure Python mode)"""
    print("\n" + "="*70)
    print("TEST 4: Zero API Keys Mode (Pure Python)")
    print("="*70)
    
    # Temporarily clear all API keys
    original_env = {}
    keys_to_clear = [
        'GEMINI_API_KEY_1', 'GEMINI_API_KEY_2',
        'GROQ_API_KEY_1', 'GROQ_API_KEY_2',
        'MISTRAL_API_KEY_1', 'MISTRAL_API_KEY_2',
        'OPENROUTER_API_KEY'
    ]
    
    for key in keys_to_clear:
        original_env[key] = os.environ.get(key)
        if key in os.environ:
            del os.environ[key]
    
    try:
        # Create test dataset
        df = pd.DataFrame({
            'sales': [100, 200, 150, 300, 250, 180, 220, 190],
            'quantity': [10, 20, 15, 30, 25, 18, 22, 19],
            'profit': [50, 100, 75, 150, 125, 90, 110, 95]
        })
        
        # Test Python selection (should work)
        kpis = select_kpis_python(df)
        assert len(kpis) == 3, "Python selection must work without API keys"
        
        # Test enrichment (should use fallback)
        llm_manager = LLMManager()
        dataset_context = {'filename': 'test.csv', 'rows': len(df), 'columns': list(df.columns)}
        enriched = enrich_kpis(kpis, dataset_context, llm_manager)
        assert len(enriched) == 3, "Enrichment fallback must work without API keys"
        
        print(f"✓ System works with ZERO API keys")
        print(f"  - KPIs selected: {kpis}")
        print(f"  - Enrichment used Python fallback")
        print("✓ Test PASSED: Pure Python mode works")
        
    finally:
        # Restore original environment
        for key, value in original_env.items():
            if value is not None:
                os.environ[key] = value


if __name__ == "__main__":
    print("\n" + "="*70)
    print("  PHASE 0B MULTI-PROVIDER LLM ORCHESTRATION TESTS")
    print("  Testing: Python-First + Multi-Provider Fallback")
    print("="*70)
    
    try:
        test_python_kpi_selection()
        test_llm_enrichment_with_fallback()
        test_complete_phase_0b_flow()
        test_zero_api_keys_mode()
        
        print("\n" + "="*70)
        print("  ALL TESTS PASSED ✓")
        print("  Phase 0B Multi-Provider Orchestration: WORKING")
        print("="*70 + "\n")
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
