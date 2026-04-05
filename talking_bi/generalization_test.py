import asyncio
import pandas as pd
from uuid import uuid4
from services.orchestrator import QueryOrchestrator
from services.session_manager import create_session
from models.contracts import UploadedDataset
import json

def log_test(dataset_name, query, expected, actual_status, actual_res, passed):
    status_str = "PASS" if passed else "FAIL"
    print(f"\n--- DATASET: {dataset_name} | QUERY: {query}")
    print(f"EXPECTED: {expected}")
    print(f"ACTUAL: {actual_status} | KPI: {actual_res.get('kpi')} | DIM: {actual_res.get('dimension')}")
    print(f"STATUS: {status_str}")

def run_generalization_suite():
    orch = QueryOrchestrator()

    # 1. HR Dataset
    print("\n=== TESTING HR DATASET ===")
    df_hr = pd.DataFrame({
        'employee_id': [101, 102, 103],
        'salary': [70000, 80000, 90000],
        'department': ['IT', 'Sales', 'HR'],
        'joining_date': pd.to_datetime(['2020-01-01', '2021-01-01', '2022-01-01'])
    })
    meta_hr = UploadedDataset(
        session_id=str(uuid4()), filename="hr.csv", columns=list(df_hr.columns),
        dtypes={c: str(df_hr[c].dtype) for c in df_hr.columns}, shape=df_hr.shape,
        sample_values={c: df_hr[c].astype(str).unique()[:3].tolist() for c in df_hr.columns},
        missing_pct={c: 0.0 for c in df_hr.columns}
    )
    sid_hr = create_session(df_hr, metadata=meta_hr)
    
    # Domain Shift: "show salary by department"
    res_hr1 = orch.handle("show salary by department", sid_hr)
    log_test("HR", "show salary by department", "salary by department", res_hr1.status, res_hr1.intent, 
             res_hr1.intent.get('kpi') == 'salary' and res_hr1.intent.get('dimension') == 'department')

    # Synonym: "show compensation"
    res_hr2 = orch.handle("show compensation", sid_hr)
    log_test("HR", "show compensation", "salary mapping or AMBIGUOUS", res_hr2.status, res_hr2.intent, 
             res_hr2.status in ["RESOLVED", "AMBIGUOUS"])

    # 2. Product Dataset
    print("\n=== TESTING PRODUCT DATASET ===")
    df_prod = pd.DataFrame({
        'product_id': ['P1', 'P2', 'P3'],
        'units_sold': [100, 150, 200],
        'category': ['Electronics', 'Home', 'Toys'],
        'price': [500, 20, 15]
    })
    meta_prod = UploadedDataset(
        session_id=str(uuid4()), filename="products.csv", columns=list(df_prod.columns),
        dtypes={c: str(df_prod[c].dtype) for c in df_prod.columns}, shape=df_prod.shape,
        sample_values={c: df_prod[c].astype(str).unique()[:3].tolist() for c in df_prod.columns},
        missing_pct={c: 0.0 for c in df_prod.columns}
    )
    sid_prod = create_session(df_prod, metadata=meta_prod)
    
    # Domain Shift: "show units_sold by category"
    res_prod1 = orch.handle("show units_sold by category", sid_prod)
    log_test("Product", "show units_sold by category", "units_sold by category", res_prod1.status, res_prod1.intent, 
             res_prod1.intent.get('kpi') == 'units_sold' and res_prod1.intent.get('dimension') == 'category')

    # 3. Messy Dataset
    print("\n=== TESTING MESSY DATASET ===")
    df_messy = pd.DataFrame({
        'rev_amt': [1000, 2000, 3000],
        'custID': ['C1', 'C2', 'C3'],
        'dt': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'val_pct': [0.1, 0.2, 0.3],
        'misc_col': ['x', 'y', 'z']
    })
    meta_messy = UploadedDataset(
        session_id=str(uuid4()), filename="messy.csv", columns=list(df_messy.columns),
        dtypes={c: str(df_messy[c].dtype) for c in df_messy.columns}, shape=df_messy.shape,
        sample_values={c: df_messy[c].astype(str).unique()[:3].tolist() for c in df_messy.columns},
        missing_pct={c: 0.0 for c in df_messy.columns}
    )
    sid_messy = create_session(df_messy, metadata=meta_messy)
    
    # Messy: "show revenue"
    res_messy1 = orch.handle("show revenue", sid_messy)
    log_test("Messy", "show revenue", "mapped to rev_amt or AMBIGUOUS", res_messy1.status, res_messy1.intent, 
             res_messy1.intent.get('kpi') == 'rev_amt' or res_messy1.status == "AMBIGUOUS")

    # Messy: "show by customer"
    res_messy2 = orch.handle("show by customer", sid_messy)
    log_test("Messy", "show by customer", "mapped to custID or AMBIGUOUS (likely blocked as ID)", res_messy2.status, res_messy2.intent, 
             res_messy2.intent.get('dimension') == 'custID' or res_messy2.status in ["AMBIGUOUS", "INVALID", "UNKNOWN"])

    # 4. Context Chaining on HR (Long Context)
    print("\n=== TESTING LONG CONTEXT ON HR ===")
    steps = [
        ("show salary", "RESOLVED"),
        ("by department", "RESOLVED"),
        ("filter IT", "RESOLVED"),
        ("show trends", "RESOLVED")
    ]
    for q, exp in steps:
        res = orch.handle(q, sid_hr)
        print(f"STEP: {q} -> STATUS: {res.status} | KPI: {res.intent.get('kpi')} | DIM: {res.intent.get('dimension')}")

if __name__ == "__main__":
    run_generalization_suite()
