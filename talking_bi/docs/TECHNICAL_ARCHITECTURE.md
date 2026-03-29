# Talking BI - Technical Architecture

## 🏛️ System Architecture

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │   Web    │  │  Mobile  │  │   CLI    │  │   API    │       │
│  │  Client  │  │   App    │  │  Client  │  │  Client  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
└───────┼─────────────┼─────────────┼─────────────┼──────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                      │
        ┌─────────────▼─────────────┐
        │      FastAPI Server       │
        │    (Uvicorn ASGI)         │
        └─────────────┬─────────────┘
                      │
        ┌─────────────▼─────────────────────────────────────┐
        │              API LAYER                             │
        │  ┌──────────────┐      ┌──────────────────┐       │
        │  │   Upload     │      │  Intelligence    │       │
        │  │   Router     │      │     Router       │       │
        │  │ POST /upload │      │ POST /intel/{id} │       │
        │  └──────┬───────┘      └────────┬─────────┘       │
        └─────────┼──────────────────────┼──────────────────┘
                  │                      │
        ┌─────────▼──────────────────────▼─────────────────┐
        │           SERVICE LAYER                           │
        │  ┌────────────────┐  ┌──────────────────────┐    │
        │  │    Session     │  │   Intelligence       │    │
        │  │    Manager     │  │     Engine           │    │
        │  └────────┬───────┘  └──────────┬───────────┘    │
        │           │                      │                │
        │  ┌────────▼───────┐  ┌──────────▼───────────┐    │
        │  │   Scheduler    │  │  Dataset Profiler    │    │
        │  │  (APScheduler) │  │  (Python Analysis)   │    │
        │  └────────────────┘  └──────────┬───────────┘    │
        │                                  │                │
        │                      ┌───────────▼───────────┐    │
        │                      │   KPI Selector        │    │
        │                      │  (Python-First)       │    │
        │                      └───────────┬───────────┘    │
        │                                  │                │
        │                      ┌───────────▼───────────┐    │
        │                      │   LLM Manager         │    │
        │                      │ (Multi-Provider)      │    │
        │                      └───────────┬───────────┘    │
        │                                  │                │
        │           ┌──────────────────────┼────────────┐   │
        │           │                      │            │   │
        │  ┌────────▼────────┐  ┌─────────▼────────┐  │   │
        │  │ KPI Enrichment  │  │  Dashboard       │  │   │
        │  │ (LLM Optional)  │  │   Planner        │  │   │
        │  └─────────────────┘  └──────────────────┘  │   │
        └─────────────────────────────────────────────┼───┘
                                                      │
        ┌─────────────────────────────────────────────▼───┐
        │              LLM PROVIDER LAYER                  │
        │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
        │  │  Gemini  │→ │   Groq   │→ │ Mistral  │→     │
        │  │ (2 keys) │  │ (2 keys) │  │ (2 keys) │      │
        │  └──────────┘  └──────────┘  └──────────┘      │
        │                                                  │
        │  ┌──────────┐  ┌──────────────────────────┐    │
        │  │OpenRouter│→ │   Python Fallback        │    │
        │  │ (1 key)  │  │  (Always Works)          │    │
        │  └──────────┘  └──────────────────────────┘    │
        └──────────────────────────────────────────────────┘
                                                      
        ┌──────────────────────────────────────────────────┐
        │              DATA LAYER                          │
        │  ┌──────────────┐  ┌──────────────────────┐     │
        │  │  In-Memory   │  │   Pandas DataFrames  │     │
        │  │   Sessions   │  │   (Processing)       │     │
        │  │  (24h TTL)   │  │                      │     │
        │  └──────────────┘  └──────────────────────┘     │
        └──────────────────────────────────────────────────┘
```

## 🔧 Component Details

### 1. API Layer

#### FastAPI Application
```python
app = FastAPI(
    title="Talking BI",
    version="0.2.0",
    lifespan=lifespan  # Startup/shutdown hooks
)
```

**Features**:
- Async request handling
- Automatic OpenAPI documentation
- Request validation via Pydantic
- CORS support (configurable)
- Health check endpoint

#### Upload Router (`api/upload.py`)
```python
@router.post("/upload")
async def upload_csv(file: UploadFile)
```

**Responsibilities**:
- File type validation (CSV only)
- File size validation (≤ 10MB)
- CSV parsing with Pandas
- Column normalization
- Session creation
- Metadata extraction

**Response**:
```json
{
    "session_id": "uuid",
    "message": "success",
    "filename": "data.csv",
    "columns": ["col1", "col2"]
}
```

#### Intelligence Router (`api/intelligence.py`)
```python
@router.post("/intelligence/{session_id}")
async def generate_intelligence(session_id: str)
```

**Responsibilities**:
- Session validation
- Dataset profiling
- KPI selection
- LLM enrichment
- Dashboard plan generation

**Response**:
```json
{
    "session_id": "uuid",
    "kpis": [...],
    "charts": [...],
    "story_arc": "text",
    "kpi_coverage": 1.0
}
```

### 2. Service Layer

#### Session Manager (`services/session_manager.py`)

**Data Structure**:
```python
SESSION_STORE = {
    "session_id": {
        "df": pd.DataFrame,
        "metadata": UploadedDataset,
        "created_at": datetime,
        "expires_at": datetime
    }
}
```

**Functions**:
- `create_session(df, metadata)` → session_id
- `get_session(session_id)` → session_data
- `delete_session(session_id)` → bool
- `cleanup_expired_sessions()` → int (deleted count)
- `start_cleanup_scheduler()` → scheduler

**Cleanup Strategy**:
- Background job runs every 10 minutes
- Deletes sessions older than 24 hours
- Logs cleanup operations

#### Dataset Profiler (`services/dataset_profiler.py`)

**Analysis Pipeline**:
```python
def profile_dataset(df: pd.DataFrame) -> Dict:
    1. Detect column types (numeric, categorical, datetime)
    2. Calculate cardinality (unique values)
    3. Compute missing percentages
    4. Generate statistical summaries
    5. Identify potential KPI columns
    6. Extract segmentation candidates
    7. Find time columns
```

**Output**:
```python
{
    "numeric_cols": ["sales", "quantity"],
    "categorical_cols": ["region", "product"],
    "datetime_cols": ["date"],
    "cardinality": {"region": 4, "product": 100},
    "missing_pct": {"sales": 0.1, "quantity": 0.0},
    "stats": {"sales": {"mean": 1000, "std": 200}}
}
```

#### KPI Selector (`services/kpi_selector.py`)

**Algorithm** (Python-First):
```python
def select_kpis_python(df: pd.DataFrame) -> List[str]:
    # Step 1: Get numeric columns
    numeric_cols = df.select_dtypes(include=["int", "float"]).columns
    
    # Step 2: Filter valid columns
    valid = []
    for col in numeric_cols:
        if df[col].nunique() > 5 and df[col].isna().mean() < 0.3:
            valid.append(col)
    
    # Step 3: Take first 3
    kpis = valid[:3]
    
    # Step 4: Fallback if needed
    if len(kpis) < 3:
        kpis.extend(numeric_cols[:3-len(kpis)])
    
    # Step 5: Final fallback
    if len(kpis) < 3:
        kpis.extend(df.columns[:3-len(kpis)])
    
    # Step 6: Ensure exactly 3
    return kpis[:3]
```

**Guarantees**:
- ✅ ALWAYS returns exactly 3 KPIs
- ✅ Works without any LLM
- ✅ Deterministic (same input → same output)
- ✅ Fast (< 50ms)

#### LLM Manager (`services/llm_manager.py`)

**Multi-Provider Architecture**:
```python
class LLMManager:
    providers = [
        ("gemini", [key1, key2]),
        ("groq", [key1, key2]),
        ("mistral", [key1, key2]),
        ("openrouter", [key1])
    ]
    
    def call_llm(prompt: str) -> Optional[str]:
        for provider, keys in providers:
            for key in keys:
                try:
                    return _call_provider(provider, key, prompt)
                except Exception:
                    continue
        return None  # All failed
```

**Provider Implementations**:

1. **Gemini** (Priority 1)
```python
def _call_gemini(key: str, prompt: str) -> str:
    genai.configure(api_key=key)
    model = genai.GenerativeModel('gemini-pro-latest')
    response = model.generate_content(prompt)
    return response.text
```

2. **Groq** (Priority 2)
```python
def _call_groq(key: str, prompt: str) -> str:
    client = Groq(api_key=key)
    completion = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}]
    )
    return completion.choices[0].message.content
```

3. **Mistral** (Priority 3)
```python
def _call_mistral(key: str, prompt: str) -> str:
    client = Mistral(api_key=key)
    response = client.chat.complete(
        model="mistral-small-latest",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content
```

4. **OpenRouter** (Priority 4)
```python
def _call_openrouter(key: str, prompt: str) -> str:
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "model": "meta-llama/llama-3.1-8b-instruct:free",
            "messages": [{"role": "user", "content": prompt}]
        }
    )
    return response.json()["choices"][0]["message"]["content"]
```

**Fallback Chain**:
```
Gemini Key 1 → Gemini Key 2 → Groq Key 1 → Groq Key 2 
→ Mistral Key 1 → Mistral Key 2 → OpenRouter Key → Python Fallback
```

#### KPI Enrichment (`services/kpi_enrichment.py`)

**Enrichment Pipeline**:
```python
def enrich_kpis(kpi_columns: List[str], context: Dict, llm: LLMManager):
    # Step 1: Build prompt
    prompt = build_enrichment_prompt(kpi_columns, context)
    
    # Step 2: Try LLM enrichment
    response = llm.call_llm(prompt)
    
    if response:
        try:
            # Step 3: Parse JSON response
            enriched = parse_enrichment_response(response)
            return enriched
        except Exception:
            pass
    
    # Step 4: Python fallback
    return fallback_enrichment(kpi_columns)
```

**LLM Prompt Template**:
```
You are a business intelligence expert. Convert these data columns into business KPIs.

Dataset: {filename}
Columns to enrich: {kpi_columns}

For each column, provide:
1. A business-friendly name
2. The best aggregation method (sum, avg, count, min, max)
3. A brief business meaning

Return ONLY a JSON array with this exact format:
[
  {
    "name": "descriptive business name",
    "source_column": "column_name",
    "aggregation": "sum|avg|count|min|max",
    "business_meaning": "what this measures in business terms"
  },
  ...
]
```

**Python Fallback**:
```python
def fallback_enrichment(kpi_columns: List[str]) -> List[Dict]:
    enriched = []
    for col in kpi_columns:
        kpi = {
            "name": col.replace('_', ' ').title(),
            "source_column": col,
            "aggregation": "sum",
            "business_meaning": f"Total {col.replace('_', ' ')}"
        }
        enriched.append(kpi)
    return enriched
```

#### Dashboard Planner (`services/dashboard_planner.py`)

**Planning Algorithm**:
```python
def create_dashboard_plan(session_id, kpis, context):
    # Step 1: Convert dict KPIs to KPI objects
    kpi_objects = [KPI(**kpi) for kpi in kpis]
    
    # Step 2: Generate charts for each KPI
    charts = [generate_chart_for_kpi(kpi) for kpi in kpi_objects]
    
    # Step 3: Generate story arc
    story_arc = generate_story_arc(kpi_objects, context)
    
    # Step 4: Calculate coverage
    kpi_coverage = len(kpi_objects) / 3.0
    
    # Step 5: Create plan
    return DashboardPlan(
        session_id=session_id,
        kpis=kpi_objects,
        charts=charts,
        story_arc=story_arc,
        kpi_coverage=kpi_coverage
    )
```

**Chart Selection Logic**:
```python
def generate_chart_for_kpi(kpi: KPI) -> ChartPlan:
    if kpi.time_column:
        # Time series → line chart
        return ChartPlan(
            chart_type="line",
            x_column=kpi.time_column,
            y_column=kpi.source_column
        )
    elif kpi.segment_by:
        # Categorical → bar chart
        return ChartPlan(
            chart_type="bar",
            x_column=kpi.segment_by,
            y_column=kpi.source_column
        )
    else:
        # Default → bar chart
        return ChartPlan(
            chart_type="bar",
            x_column=kpi.source_column,
            y_column=kpi.source_column
        )
```

#### Intelligence Engine (`services/intelligence_engine.py`)

**Orchestration Flow**:
```python
def generate_dashboard_plan(session_id, df, uploaded_dataset):
    # Step 1: Profile Dataset (Python-only)
    profile = profile_dataset(df)
    
    # Step 2: Python-First KPI Selection (PRIMARY)
    kpi_columns = select_kpis_python(df)
    assert len(kpi_columns) == 3
    
    # Step 3: Initialize Multi-Provider LLM Manager
    llm_manager = LLMManager()
    
    # Step 4: LLM Enrichment (OPTIONAL)
    dataset_context = build_context(uploaded_dataset)
    enriched_kpis = enrich_kpis(kpi_columns, dataset_context, llm_manager)
    
    # Step 5: Create Dashboard Plan
    dashboard_plan = create_dashboard_plan(
        session_id=session_id,
        kpis=enriched_kpis,
        dataset_context=dataset_context
    )
    
    return dashboard_plan
```

### 3. Model Layer

#### Data Contracts (`models/contracts.py`)

**UploadedDataset**:
```python
class UploadedDataset(BaseModel):
    session_id: str
    filename: str
    columns: List[str]
    dtypes: Dict[str, str]
    shape: Tuple[int, int]
    sample_values: Dict[str, List[str]]
    missing_pct: Dict[str, float]
```

#### Dashboard Models (`models/dashboard.py`)

**KPI**:
```python
class KPI(BaseModel):
    name: str
    source_column: str
    aggregation: str
    segment_by: Optional[str] = None
    time_column: Optional[str] = None
    business_meaning: str
    confidence: float
```

**ChartPlan**:
```python
class ChartPlan(BaseModel):
    chart_type: str
    title: str
    x_column: str
    y_column: str
    kpi_name: str
    aggregation: str
    segment_by: Optional[str] = None
```

**DashboardPlan**:
```python
class DashboardPlan(BaseModel):
    session_id: str
    kpis: List[KPI]
    charts: List[ChartPlan]
    story_arc: str
    kpi_coverage: float
    created_at: str
```

## 🔄 Request Flow

### Upload Flow

```
1. Client → POST /upload (CSV file)
2. API validates file (type, size)
3. Pandas reads CSV
4. Columns normalized (spaces → underscores)
5. Metadata extracted (dtypes, samples, missing %)
6. Session created (UUID, 24h TTL)
7. DataFrame + metadata stored in memory
8. Response → {session_id, filename, columns}
```

### Intelligence Flow

```
1. Client → POST /intelligence/{session_id}
2. API validates session exists
3. Retrieve DataFrame + metadata
4. Dataset Profiler analyzes data
5. KPI Selector (Python) → 3 KPIs
6. LLM Manager tries enrichment (Gemini → Groq → Mistral → OpenRouter)
7. If LLM fails → Python fallback enrichment
8. Dashboard Planner generates charts + story
9. Response → {kpis, charts, story_arc, coverage}
```

## 🛡️ Error Handling

### Layered Error Handling

**Level 1: API Layer**
```python
try:
    result = process_request()
    return result
except HTTPException:
    raise  # Re-raise HTTP exceptions
except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
```

**Level 2: Service Layer**
```python
try:
    result = call_external_service()
    return result
except ExternalServiceError:
    return fallback_result()
```

**Level 3: LLM Layer**
```python
for provider in providers:
    try:
        return call_provider(provider)
    except Exception:
        continue  # Try next provider
return python_fallback()  # All failed
```

### Error Types

1. **Validation Errors** (400)
   - Invalid file type
   - File too large
   - Missing required fields

2. **Not Found Errors** (404)
   - Session not found
   - Session expired

3. **Server Errors** (500)
   - Pandas parsing error
   - Unexpected exceptions

4. **LLM Errors** (handled internally)
   - Quota exceeded → try next provider
   - API timeout → try next provider
   - Invalid response → Python fallback

## 🔐 Security

### Input Validation

**File Upload**:
- Type check: Only CSV allowed
- Size check: ≤ 10MB
- Content validation: Valid CSV format

**Session ID**:
- UUID format validation
- Existence check
- Expiry check

### Data Protection

**In-Memory Storage**:
- No disk persistence
- Automatic expiry (24h)
- No external logging

**API Keys**:
- Stored in `.env` (not committed)
- Never logged
- Never returned in responses

### Rate Limiting (Future)

```python
# TODO: Implement rate limiting
@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    # Check rate limit
    # Return 429 if exceeded
    pass
```

## 📊 Performance Optimization

### Caching Strategy

**LLM Response Caching**:
```python
class LLMManager:
    cache = {}
    
    def call_llm(prompt, cache_key=None):
        if cache_key and cache_key in cache:
            return cache[cache_key]
        
        response = _call_provider(...)
        
        if cache_key:
            cache[cache_key] = response
        
        return response
```

### Async Operations

**FastAPI Async Endpoints**:
```python
@router.post("/upload")
async def upload_csv(file: UploadFile):
    # Async file reading
    content = await file.read()
    # Sync processing in thread pool
    result = await run_in_threadpool(process_csv, content)
    return result
```

### Memory Management

**Session Cleanup**:
- Background job every 10 minutes
- Deletes expired sessions
- Frees DataFrame memory

**DataFrame Optimization**:
- Dtype inference for memory efficiency
- Sample values limited to 3 per column
- No unnecessary copies

## 🧪 Testing Strategy

### Unit Tests

**Test Coverage**:
- ✅ Session management
- ✅ Dataset profiling
- ✅ KPI selection (Python-first)
- ✅ LLM manager (multi-provider)
- ✅ KPI enrichment (with fallback)
- ✅ Dashboard planning

### Integration Tests

**API Tests**:
- ✅ Upload endpoint
- ✅ Intelligence endpoint
- ✅ Health check
- ✅ Error handling

### End-to-End Tests

**Complete Flow**:
- ✅ Upload → Intelligence → Response
- ✅ Multi-provider fallback
- ✅ Zero API keys mode
- ✅ Session expiry

## 📈 Monitoring (Future)

### Metrics to Track

**Performance**:
- Request latency (p50, p95, p99)
- LLM response time
- Session creation rate
- Memory usage

**Reliability**:
- Error rate by endpoint
- LLM provider success rate
- Fallback usage frequency
- Session expiry rate

**Business**:
- Daily active sessions
- Average KPIs per session
- Chart generation success rate
- User satisfaction (RLHF)

### Logging Strategy

**Current**:
```python
print(f"[COMPONENT] Message")
```

**Future** (Structured Logging):
```python
logger.info("event", 
    component="kpi_selector",
    session_id=session_id,
    kpis_selected=3,
    duration_ms=45
)
```

## 🚀 Deployment

### Development
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Production (Future)
```bash
# With Gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker

# With Docker
docker build -t talking-bi .
docker run -p 8000:8000 talking-bi
```

### Environment Variables
```env
# Required
SESSION_EXPIRY_HOURS=24
MAX_FILE_SIZE_MB=10

# Optional (LLM providers)
GEMINI_API_KEY_1=...
GROQ_API_KEY_1=...
# ... etc
```

---

**Last Updated**: March 29, 2026
**Version**: 0.2.0
**Status**: Production-Ready (Phases 0A & 0B)
