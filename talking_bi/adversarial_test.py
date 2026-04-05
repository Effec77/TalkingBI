import asyncio
import pandas as pd
from uuid import uuid4
from services.orchestrator import QueryOrchestrator
from services.session_manager import create_session
import json

def log_test(query, expected, actual_status, actual_res, passed):
    status_str = "PASS" if passed else "FAIL"
    print(f"\n--- TESTING: {query}")
    print(f"EXPECTED: {expected}")
    print(f"ACTUAL: {actual_status} | KPI: {actual_res.get('kpi')} | DIM: {actual_res.get('dimension')}")
    print(f"STATUS: {status_str}")
    if not passed:
        print(f"DEBUG: {actual_res}")

def run_adversarial_suite():
    # Setup dataset
    df = pd.DataFrame({
        'revenue': [100, 200, 300],
        'sales': [110, 210, 310],
        'profit': [50, 60, 70],
        'cost': [40, 50, 60],
        'user_id': [1, 2, 3],
        'region': ['North', 'South', 'East'],
        'department': ['IT', 'Sales', 'HR'],
        'hiring_date': pd.to_datetime(['2020-01-01', '2021-01-01', '2022-01-01'])
    })
    
    from models.contracts import UploadedDataset
    metadata = UploadedDataset(
        session_id=str(uuid4()),
        filename="adversarial.csv",
        columns=list(df.columns),
        dtypes={col: str(df[col].dtype) for col in df.columns},
        shape=df.shape,
        sample_values={col: df[col].astype(str).unique()[:3].tolist() for col in df.columns},
        missing_pct={col: 0.0 for col in df.columns}
    )
    session_id = create_session(df, metadata=metadata)
    orch = QueryOrchestrator()

    # 1. SPELLING / NLP ATTACKS
    q1 = "revnue by regoin"
    res1 = orch.handle(q1, session_id)
    log_test(q1, "Corrected to revenue by region", res1.status, res1.intent, res1.intent.get('kpi') == 'revenue' and res1.intent.get('dimension') == 'region')

    # 2. AMBIGUITY TESTS
    q2 = "show amount"
    res2 = orch.handle(q2, session_id)
    # Since amount is a common synonym for revenue/sales/cost, it should be ambiguous or resolved to a top choice depending on role_scores
    # But with 4 numeric columns, it's likely ambiguous.
    log_test(q2, "AMBIGUOUS (Multiple candidates)", res2.status, res2.intent, res2.status == "AMBIGUOUS")

    # 3. INCOMPLETE QUERIES
    q3 = "show by region"
    res3 = orch.handle(q3, session_id)
    log_test(q3, "INCOMPLETE (Missing KPI)", res3.status, res3.intent, res3.status == "INCOMPLETE")

    # 4. INVALID OPERATIONS
    q4 = "show revenue by user_id"
    res4 = orch.handle(q4, session_id)
    log_test(q4, "INVALID or BLOCKED (Identifier penalty)", res4.status, res4.intent, res4.status in ["INVALID", "UNKNOWN"])

    # 5. CONTEXT DRIFT TEST
    print("\n--- Testing Context Drift Sequence ---")
    steps = [
        ("show revenue", "RESOLVED"),
        ("by region", "RESOLVED"),
        ("compare with profit", "RESOLVED"),
        ("show trends", "RESOLVED")
    ]
    for q, exp_status in steps:
        res = orch.handle(q, session_id)
        print(f"STEP: {q} -> STATUS: {res.status} | KPI: {res.intent.get('kpi')} | DIM: {res.intent.get('dimension')}")

    # 6. ATTACK: Mixed Noise
    q6 = "shwo value in IT"
    res6 = orch.handle(q6, session_id)
    log_test(q6, "AMBIGUOUS or INCOMPLETE (Spelling + Filter interpretation)", res6.status, res6.intent, res6.status in ["AMBIGUOUS", "INCOMPLETE", "RESOLVED"])
    
if __name__ == "__main__":
    run_adversarial_suite()
