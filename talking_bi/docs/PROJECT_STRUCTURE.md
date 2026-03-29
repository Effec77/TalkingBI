# Project Structure

This document explains the organization of the Talking BI Phase 0A codebase.

---

## Directory Layout

```
talking_bi/
├── 📁 api/                      # API Layer - HTTP endpoints
│   ├── __init__.py
│   └── upload.py               # CSV upload endpoint handler
│
├── 📁 models/                   # Data Models - Contracts & schemas
│   ├── __init__.py
│   └── contracts.py            # UploadedDataset dataclass
│
├── 📁 services/                 # Business Logic Layer
│   ├── __init__.py
│   └── session_manager.py      # Session lifecycle management
│
├── 📁 tests/                    # Test Suite
│   ├── __init__.py
│   └── test_api.py             # API integration tests
│
├── 📁 data/                     # Data Files
│   ├── .gitkeep                # Keep empty directory in git
│   ├── test_data.csv           # Sample test dataset
│   └── empty.csv               # Empty CSV for error testing
│
├── 📁 docs/                     # Documentation
│   ├── API_REFERENCE.md        # Complete API documentation
│   ├── SETUP_GUIDE.md          # Installation & setup instructions
│   └── PROJECT_STRUCTURE.md    # This file
│
├── 📁 venv/                     # Virtual Environment (gitignored)
│   └── ...                     # Python packages
│
├── main.py                      # Application Entry Point
├── requirements.txt             # Python Dependencies
├── .env                         # Environment Config (gitignored)
├── .env.example                # Environment Template
├── .gitignore                  # Git Ignore Rules
└── README.md                   # Project Overview
```

---

## Layer Architecture

### 1. Entry Point (`main.py`)

**Purpose:** Initialize FastAPI application and configure startup/shutdown

**Responsibilities:**
- Create FastAPI app instance
- Register API routers
- Configure lifespan events
- Start background scheduler

**Key Code:**
```python
app = FastAPI(lifespan=lifespan)
app.include_router(upload_router)
```

---

### 2. API Layer (`api/`)

**Purpose:** Handle HTTP requests and responses

**Files:**
- `upload.py` - CSV upload endpoint

**Responsibilities:**
- Request validation
- File handling
- Response formatting
- Error handling

**Key Functions:**
- `upload_csv()` - POST /upload endpoint
- `extract_metadata()` - Extract dataset information

---

### 3. Models Layer (`models/`)

**Purpose:** Define data structures and contracts

**Files:**
- `contracts.py` - Data contracts

**Responsibilities:**
- Define immutable data structures
- Type safety
- Data validation

**Key Classes:**
- `UploadedDataset` - Frozen dataclass for upload response

---

### 4. Services Layer (`services/`)

**Purpose:** Business logic and core functionality

**Files:**
- `session_manager.py` - Session management

**Responsibilities:**
- Session CRUD operations
- Expiry management
- Background cleanup
- In-memory storage

**Key Functions:**
- `create_session()` - Create new session
- `get_session()` - Retrieve session
- `delete_session()` - Remove session
- `cleanup_expired_sessions()` - Background cleanup
- `start_cleanup_scheduler()` - Initialize scheduler

---

### 5. Tests Layer (`tests/`)

**Purpose:** Automated testing

**Files:**
- `test_api.py` - API integration tests

**Responsibilities:**
- Endpoint testing
- Error case validation
- Integration testing

**Test Coverage:**
- Health endpoint
- Valid CSV upload
- Invalid file type
- Empty CSV handling

---

### 6. Data Layer (`data/`)

**Purpose:** Store sample and test data files

**Files:**
- `test_data.csv` - Valid sample dataset
- `empty.csv` - Empty CSV for testing
- `.gitkeep` - Preserve directory in git

**Usage:**
- Development testing
- Example data for documentation
- Test suite fixtures

---

### 7. Documentation Layer (`docs/`)

**Purpose:** Comprehensive project documentation

**Files:**
- `API_REFERENCE.md` - Complete API documentation
- `SETUP_GUIDE.md` - Installation instructions
- `PROJECT_STRUCTURE.md` - This file

**Content:**
- API endpoints and examples
- Setup instructions
- Architecture overview
- Troubleshooting guides

---

## Configuration Files

### `.env`
**Purpose:** Environment-specific configuration (gitignored)

**Contains:**
- Session expiry settings
- File size limits
- Cleanup intervals
- API keys (future phases)

**Security:** Never commit this file!

### `.env.example`
**Purpose:** Template for environment configuration

**Contains:**
- Example configuration values
- Documentation for each variable
- No sensitive data

**Usage:** Copy to `.env` and customize

### `.gitignore`
**Purpose:** Specify files to exclude from version control

**Excludes:**
- Virtual environment (`venv/`)
- Environment config (`.env`)
- Python cache (`__pycache__/`)
- IDE files (`.vscode/`, `.idea/`)
- OS files (`.DS_Store`)

### `requirements.txt`
**Purpose:** Python package dependencies

**Contains:**
- Package names with versions
- Direct dependencies only

**Usage:** `pip install -r requirements.txt`

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                         Client Request                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    main.py (Entry Point)                     │
│  - FastAPI app initialization                                │
│  - Router registration                                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   api/upload.py (API Layer)                  │
│  - Validate file (extension, size)                           │
│  - Parse CSV with pandas                                     │
│  - Normalize column names                                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│          services/session_manager.py (Service Layer)         │
│  - Generate UUID                                             │
│  - Store DataFrame in SESSION_STORE                          │
│  - Set expiry timestamp                                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              api/upload.py (Metadata Extraction)             │
│  - Extract shape, columns, dtypes                            │
│  - Calculate missing percentages                             │
│  - Get sample values                                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           models/contracts.py (Data Contract)                │
│  - Create UploadedDataset instance                           │
│  - Ensure type safety                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    JSON Response to Client                   │
│  - session_id                                                │
│  - dataset metadata                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Design Principles

### 1. Separation of Concerns
- Each layer has a single responsibility
- API layer handles HTTP, services handle logic
- Models define data structures

### 2. Modularity
- Each module can be tested independently
- Easy to extend with new features
- Clear dependencies between layers

### 3. Type Safety
- Frozen dataclasses for immutability
- Type hints throughout codebase
- Pydantic models (via FastAPI)

### 4. Security First
- Environment variables for configuration
- `.env` file gitignored
- Input validation at API layer

### 5. Testability
- Separate test directory
- Integration tests for API
- Sample data for testing

### 6. Documentation
- Comprehensive README
- API reference documentation
- Setup guides
- Inline code comments

---

## Adding New Features

### Adding a New Endpoint

1. **Create handler in `api/`**
```python
# api/query.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/query/{session_id}")
async def query_data(session_id: str):
    # Implementation
    pass
```

2. **Register router in `main.py`**
```python
from api.query import router as query_router
app.include_router(query_router)
```

3. **Add tests in `tests/`**
```python
# tests/test_query.py
def test_query_endpoint():
    # Test implementation
    pass
```

4. **Update documentation in `docs/`**

### Adding a New Service

1. **Create service file in `services/`**
```python
# services/analytics.py
def calculate_statistics(df):
    # Implementation
    pass
```

2. **Import in API layer**
```python
from services.analytics import calculate_statistics
```

3. **Add tests**
4. **Update documentation**

---

## Best Practices

### Code Organization
- ✅ Keep related code together
- ✅ One class/function per responsibility
- ✅ Use meaningful names
- ✅ Add docstrings to functions

### File Management
- ✅ All docs in `docs/`
- ✅ All tests in `tests/`
- ✅ All data in `data/`
- ✅ No loose files in root

### Version Control
- ✅ Commit `.env.example`, not `.env`
- ✅ Use `.gitignore` properly
- ✅ Keep commits focused and atomic
- ✅ Write descriptive commit messages

### Testing
- ✅ Test all endpoints
- ✅ Test error cases
- ✅ Use sample data from `data/`
- ✅ Run tests before committing

---

## Future Expansion

As the project grows, consider:

### Phase 0B - Database Layer
```
├── 📁 database/
│   ├── __init__.py
│   ├── connection.py
│   └── models.py
```

### Phase 1 - LLM Integration
```
├── 📁 llm/
│   ├── __init__.py
│   ├── client.py
│   └── prompts.py
```

### Phase 2 - Analytics
```
├── 📁 analytics/
│   ├── __init__.py
│   ├── kpi.py
│   └── calculations.py
```

---

## Maintenance

### Regular Tasks
- Update dependencies: `pip install --upgrade -r requirements.txt`
- Run tests: `python tests/test_api.py`
- Check for security issues: `pip audit`
- Update documentation when adding features

### Code Quality
- Use linters: `pylint`, `flake8`
- Format code: `black`
- Type checking: `mypy`

---

**This structure ensures the project remains organized, maintainable, and scalable as it grows through future phases.**
