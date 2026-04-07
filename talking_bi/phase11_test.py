import pandas as pd
from services.dataset_intelligence import DatasetIntelligence
from services.dashboard_generator import generate_auto_dashboard
from services.insight_engine import generate_insights
from services.query_suggester import generate_suggestions

def evaluate(df, name):
    print(f"\n=== Testing dataset: {name} ===")
    rows, cols = df.shape
    print(f"Rows: {rows}, Columns: {cols}")
    profile = DatasetIntelligence(df).build()
    # Dashboard
    dashboard = generate_auto_dashboard(df, profile)
    kpis = dashboard.get('kpis', [])
    charts = dashboard.get('charts', [])
    print(f"Dashboard -> KPI cards: {len(kpis)}, Charts: {len(charts)}")
    # Checks
    if len(charts) > 4:
        print("FAIL: Too many charts (>4)")
    # Insight
    insights = generate_insights(df, profile, dashboard).get('insights', [])
    print(f"Insights count: {len(insights)}")
    if len(insights) > 5:
        print("FAIL: Too many insights (>5)")
    # Suggestions
    suggestions = generate_suggestions(profile).get('suggestions', [])
    print(f"Suggestions count: {len(suggestions)}")
    if len(suggestions) > 8:
        print("FAIL: Too many suggestions (>8)")
    # Additional checks
    # No identifier dimension in charts
    for ch in charts:
        dim = ch.get('dimension')
        if dim:
            meta = profile.get(dim, {})
            if str(meta.get('semantic_type', '')).lower() == 'identifier' or dim.lower().endswith('_id'):
                print(f"FAIL: Identifier used as dimension in chart: {dim}")
    # Trend chart only if time column exists
    has_time = any(str(meta.get('semantic_type','')).lower() in ('date','datetime','time') or meta.get('role_scores',{}).get('is_date',0)==1.0 for meta in profile.values())
    trend_charts = [c for c in charts if c.get('type')=='line']
    if trend_charts and not has_time:
        print("FAIL: Trend chart generated without time column")
    print("Insights:")
    for i in insights:
        print(f" - {i}")
    print("Suggestions:")
    for s in suggestions:
        print(f" - {s}")

if __name__ == "__main__":
    # Clean dataset (test.csv)
    clean_path = "d:/datasets/test.csv"  # will copy test.csv there if needed
    try:
        clean_df = pd.read_csv(clean_path)
    except Exception:
        clean_df = pd.read_csv("d:/GitHub/TalkingBI/talking_bi/test.csv")
    evaluate(clean_df, "Clean CSV")

    # Multi‑KPI dataset (synthetic)
    multi_df = pd.DataFrame({
        "revenue": [100, 200, 150, 300],
        "profit": [20, 50, 30, 80],
        "cost": [80, 150, 120, 220],
        "region": ["North", "South", "East", "West"],
        "date": pd.date_range(start="2023-01-01", periods=4)
    })
    evaluate(multi_df, "Multi‑KPI synthetic")

    # Messy dataset (generated earlier)
    messy_path = "d:/datasets/messy_stress_test.csv"
    messy_df = pd.read_csv(messy_path)
    evaluate(messy_df, "Messy chaotic")
