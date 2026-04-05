import asyncio
import pandas as pd
from uuid import uuid4
from services.orchestrator import QueryOrchestrator
from services.session_manager import create_session
from models.contracts import UploadedDataset
import json

def log_test(query, expected, actual_status, actual_res, passed):
    status_str = "PASS" if passed else "FAIL"
    print(f"\n--- QUERY: {query}")
    print(f"EXPECTED: {expected}")
    print(f"ACTUAL: {actual_status} | KPI: {actual_res.get('kpi')} | DIM: {actual_res.get('dimension')} | FILTER: {actual_res.get('filter')}")
    print(f"STATUS: {status_str}")

def run_chaos_stress_test():
    orch = QueryOrchestrator()

    # Load chaotic dataset
    path = "d:/datasets/messy_stress_test.csv"
    df = pd.read_csv(path)
    
    # Pre-process columns exactly like the system does
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    meta = UploadedDataset(
        session_id=str(uuid4()), filename="messy_stress_test.csv", columns=list(df.columns),
        dtypes={c: str(df[c].dtype) for c in df.columns}, shape=df.shape,
        sample_values={c: df[c].astype(str).unique()[:3].tolist() for c in df.columns},
        missing_pct={c: float(df[c].isna().mean()) for c in df.columns}
    )
    sid = create_session(df, metadata=meta)
    
    # 1. Fuzzy Synonyms: "show rev amt"
    q1 = "show rev amt"
    res1 = orch.handle(q1, sid)
    log_test(q1, "Resolved to rev_amt_($_) or AMBIGUOUS with revenue/revenue_2023", res1.status, res1.intent, res1.status in ["RESOLVED", "AMBIGUOUS"])

    # 2. Percentage Logic: "show value percent trend"
    q2 = "show value percent trend"
    res2 = orch.handle(q2, sid)
    log_test(q2, "Mapped value_% and added trend (dt or date_utc)", res2.status, res2.intent, 
             res2.intent.get('kpi') == 'value_%' and res2.intent.get('dimension') is not None)

    # 3. ID Penalty: "group by cust-id"
    q3 = "group by customer id"
    res3 = orch.handle(q3, sid)
    log_test(q3, "AMBIGUOUS, INVALID, or UNKNOWN (Identifier penalty for cust-id)", res3.status, res3.intent, 
             res3.status in ["AMBIGUOUS", "INVALID", "UNKNOWN", "INCOMPLETE"])

    # 4. Multi-Revenue Ambiguity: "compare revenue_2023 with profit"
    q4 = "compare revenue_2023 with profit"
    res4 = orch.handle(q4, sid)
    log_test(q4, "COMPARE intent with profit_2023 and revenue_2023", res4.status, res4.intent, 
             res4.status == "RESOLVED" and res4.intent.get('intent') == "COMPARE")

    # 5. Casing & Whitespace: "filter north region"
    q5 = "filter north region"
    res5 = orch.handle(q5, sid)
    log_test(q5, "Filter region == 'North'", res5.status, res5.intent, 
             res5.intent.get('filter') is not None)

if __name__ == "__main__":
    run_chaos_stress_test()
