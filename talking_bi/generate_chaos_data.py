import pandas as pd
import numpy as np
import random
from uuid import uuid4

def generate_chaotic_dataset(rows=5000):
    np.random.seed(42)
    
    regions = ["North", "north", "NORTH", " South ", "East", "West", "N/A", None]
    departments = ["Sales", "IT", "H.R.", "hr", "Marketing", "SALES", "Sales & Ops", "Unknown"]
    
    data = {
        # 1. DIRTY COLUMN NAMES & SIMILAR SEMANTICS
        "Rev Amt ($)": [
            random.choice([100, 200, 300, "400", "error", "1200", None, 0.5]) 
            for _ in range(rows)
        ],
        "Revenue": [
            random.uniform(50, 500) if random.random() > 0.2 else None
            for _ in range(rows)
        ],
        "revenue_2023": [
            random.uniform(100, 1000) for _ in range(rows)
        ],
        
        # 2. IDENTIFIERS
        "cust-id": [f"CUST-{random.randint(10000, 99999)}" for _ in range(rows)],
        "order_id": [str(uuid4())[:8] for _ in range(rows)],
        
        # 3. TYPED CHAOS & NULLS
        "Value %": [
            random.choice([0.1, 0.2, "10%", "15.5", None, "N/A", 0, "0.5%"])
            for _ in range(rows)
        ],
        "Profit_2023": [
            random.uniform(-100, 100) if random.random() > 0.3 else "MISSING"
            for _ in range(rows)
        ],
        
        # 4. TIME COLUMN VARIANTS
        "Date (UTC)": [
            (pd.Timestamp("2023-01-01") + pd.Timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
            if random.random() > 0.1 else None
            for _ in range(rows)
        ],
        "dt": [
            (pd.Timestamp("2022-01-01") + pd.Timedelta(days=random.randint(0, 700))).strftime("%d/%m/%Y")
            for _ in range(rows)
        ],
        
        # 5. CATEGORICAL CHAOS
        "Region": [random.choice(regions) for _ in range(rows)],
        "Department": [random.choice(departments) for _ in range(rows)],
        
        # 6. NOISE COLUMNS
        "Unnamed: 0": list(range(rows)),
        "misc_col": [random.choice(["A", "B", "C", None]) for _ in range(rows)],
        "Notes": ["Some random note data that might mention revenue" if random.random() > 0.9 else "" for _ in range(rows)],
        "random_flag": [random.randint(0, 1) for _ in range(rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Save to CSV
    output_path = "d:/datasets/messy_stress_test.csv"
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Chaotic dataset generated at: {output_path}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

if __name__ == "__main__":
    generate_chaotic_dataset(rows=5000)
