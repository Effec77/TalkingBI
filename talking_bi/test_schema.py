import asyncio
from services.orchestrator import QueryOrchestrator
from services.session_manager import create_session
import pandas as pd
from uuid import uuid4

def run_tests():
    df = pd.DataFrame({
        'revenue': [100, 200], 'sales': [110, 210], 'profit': [50, 60],
        'id': [1, 2], 'amount': [300, 400], 'region': ['North', 'South']
    })
    metadata = {
        "kpi_candidates": [{"name": "revenue"}],
        "shape": df.shape,
        "filename": "test.csv"
    }
    session_id = create_session(df, metadata=metadata)
    
    orch = QueryOrchestrator()
    queries = [
        'show amount', 
        'show revenue by id',
        'compare profit vs sales',
        'show revnue by regoin'
    ]
    
    for q in queries:
        print('\n--- TESTING:', q)
        res = orch.handle(q, session_id)
        if hasattr(res, 'status'):
            print(f'STATUS: {res.status}')
            if res.status == 'ERROR':
                print(f'ERRORS: {res.errors}')
            print(f'MAPPED KPI: {res.intent.get("kpi")}')
            print(f'MAPPED DIM: {res.intent.get("dimension")}')
            if res.status == 'AMBIGUOUS':
                print(f'CANDIDATES: {res.candidates}')
        else:
            print(res)
            
run_tests()
