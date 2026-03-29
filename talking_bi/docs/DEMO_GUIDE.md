# Demo Guide - Talking BI Phase 0A

This guide shows how to run the comprehensive demo that showcases all Phase 0A improvements.

---

## Prerequisites

1. Server must be running:
```bash
cd talking_bi
uvicorn main:app --reload
```

2. Virtual environment activated:
```bash
# Windows
./venv/Scripts/Activate.ps1

# Linux/Mac
source venv/bin/activate
```

---

## Running the Demo

### Quick Start

```bash
cd talking_bi/tests
python demo.py
```

### What the Demo Shows

The demo runs 7 comprehensive tests showcasing all improvements:

#### 1. Health Check
- Verifies server is running
- Tests `/health` endpoint

#### 2. Basic CSV Upload
- Uploads sample CSV file
- Shows session creation
- Displays dataset metadata

#### 3. Column Normalization
- Demonstrates space → underscore conversion
- Shows: "Product Name" → "product_name"
- Verifies lowercase and trimming

#### 4. Missing Value Detection
- Uploads CSV with missing data
- Calculates missing percentages
- Shows per-column statistics

#### 5. File Validation
- Tests invalid file type rejection (.txt)
- Tests empty CSV rejection
- Verifies file size limits

#### 6. Metadata Completeness
- Shows API response format
- Confirms dtypes stored internally
- Confirms sample_values stored internally
- Verifies backward compatibility

#### 7. Logging & Observability
- Demonstrates logging output
- Shows [UPLOAD] log messages
- Verifies error logging

---

## Expected Output

```
██████████████████████████████████████████████████████████████████████
█                                                                    █
█                      TALKING BI - PHASE 0A DEMO                    █
█                   CSV Upload & Session Management                  █
█                                                                    █
██████████████████████████████████████████████████████████████████████

======================================================================
  DEMO 1: Health Check
======================================================================
GET http://localhost:8000/health
Status: 200
Response: {
  "status": "healthy"
}
✓ Server is healthy and running

======================================================================
  DEMO 2: Basic CSV Upload
======================================================================
POST http://localhost:8000/upload
File: test_data.csv
Status: 200

Response:
{
  "session_id": "4c18f909-f336-4674-8a9c-f3fcb4ac5f44",
  "dataset": {
    "filename": "test_data.csv",
    "shape": [10, 5],
    "columns": ["date", "sales", "region", "product", "quantity"],
    "missing_pct": {
      "date": 0.0,
      "sales": 0.1,
      "region": 0.0,
      "product": 0.0,
      "quantity": 0.0
    }
  }
}

✓ Session created: 4c18f909-f336-4674-8a9c-f3fcb4ac5f44
✓ Dataset shape: [10, 5]
✓ Columns: date, sales, region, product, quantity

... (continues with all 7 demos)

██████████████████████████████████████████████████████████████████████
█                                                                    █
█                            DEMO COMPLETE!                          █
█                 All improvements working correctly ✓               █
█                                                                    █
██████████████████████████████████████████████████████████████████████
```

---

## Server Logs

While the demo runs, check the server terminal for logging output:

```
[UPLOAD] File received: test_data.csv
[UPLOAD] Session created: 4c18f909-f336-4674-8a9c-f3fcb4ac5f44, shape=(10, 5)
INFO:     127.0.0.1:52573 - "POST /upload HTTP/1.1" 200 OK

[UPLOAD] File received: demo.csv
[UPLOAD] Session created: f1c5fa81-bb04-4105-b07b-eb7876d0b9c6, shape=(2, 4)
INFO:     127.0.0.1:52575 - "POST /upload HTTP/1.1" 200 OK

[UPLOAD] File received: test.txt
[UPLOAD] Error: Invalid file type - test.txt
INFO:     127.0.0.1:52581 - "POST /upload HTTP/1.1" 400 Bad Request

[UPLOAD] File received: empty.csv
[UPLOAD] Error: CSV contains no data
INFO:     127.0.0.1:52583 - "POST /upload HTTP/1.1" 400 Bad Request
```

---

## Demo Features

### Automatic Test Data Generation
The demo creates temporary CSV files for testing:
- Files with spaces in column names
- Files with missing values
- Invalid file types
- Empty files

All temporary files are cleaned up automatically.

### Comprehensive Coverage
The demo tests:
- ✅ All API endpoints
- ✅ All validation rules
- ✅ All error cases
- ✅ All improvements
- ✅ Logging output
- ✅ Metadata storage

### Visual Feedback
- Clear section headers
- Formatted JSON output
- Success indicators (✓)
- Error demonstrations
- Summary statistics

---

## Troubleshooting

### Server Not Running
```
❌ ERROR: Cannot connect to server
Please ensure the server is running:
  cd talking_bi
  uvicorn main:app --reload
```

**Solution:** Start the server in a separate terminal

### Import Errors
```
ModuleNotFoundError: No module named 'requests'
```

**Solution:** Install dependencies
```bash
pip install requests
```

### File Not Found
```
FileNotFoundError: [Errno 2] No such file or directory: 'data/test_data.csv'
```

**Solution:** Run from correct directory
```bash
cd talking_bi/tests
python demo.py
```

---

## Running Individual Tests

You can also run specific test suites:

### Original Test Suite
```bash
cd talking_bi/tests
python test_api.py
```

### Metadata Tests
```bash
cd talking_bi/tests
python test_metadata.py
```

### Improvement Tests
```bash
cd talking_bi/tests
python test_improvements.py
```

### Full Demo
```bash
cd talking_bi/tests
python demo.py
```

---

## What Gets Tested

### API Endpoints
- `GET /` - Root endpoint
- `GET /health` - Health check
- `POST /upload` - CSV upload

### Validations
- File extension (.csv only)
- File size (≤ 10MB)
- CSV format (valid structure)
- Data presence (non-empty)

### Improvements
- Column normalization (spaces → underscores)
- Metadata extraction (dtypes, sample_values)
- Missing value detection
- Logging output
- Session storage

### Error Handling
- Invalid file types
- Empty files
- Corrupted CSV
- Missing data

---

## Demo Duration

The complete demo takes approximately **10-15 seconds** to run:
- 7 test scenarios
- 1 second pause between tests
- Automatic cleanup

---

## Next Steps

After running the demo:

1. **Review server logs** - Check [UPLOAD] messages
2. **Inspect responses** - Verify JSON format
3. **Test with your data** - Upload your own CSV files
4. **Explore API** - Try different endpoints
5. **Read documentation** - Check other docs/ files

---

## Additional Resources

- **API Reference**: `docs/API_REFERENCE.md`
- **Setup Guide**: `docs/SETUP_GUIDE.md`
- **Improvements**: `docs/PHASE_0A_IMPROVEMENTS.md`
- **Project Structure**: `docs/PROJECT_STRUCTURE.md`

---

**The demo provides a complete walkthrough of all Phase 0A improvements in action! 🚀**
