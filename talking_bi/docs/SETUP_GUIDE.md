# Setup Guide

Complete guide for setting up the Talking BI Phase 0A development environment.

---

## Prerequisites

### Required Software

1. **Python 3.12 or higher**
   - Download: https://www.python.org/downloads/
   - Verify: `python --version`

2. **pip (Python package manager)**
   - Usually included with Python
   - Verify: `pip --version`

3. **Git** (optional, for cloning)
   - Download: https://git-scm.com/downloads
   - Verify: `git --version`

### Recommended Tools

- **VS Code** or **PyCharm** for development
- **Postman** or **Insomnia** for API testing
- **curl** for command-line testing

---

## Installation Steps

### 1. Get the Code

**Option A: Clone from Git**
```bash
git clone <repository-url>
cd talking_bi
```

**Option B: Download ZIP**
- Download and extract the project
- Navigate to the `talking_bi` directory

### 2. Create Virtual Environment

**Windows (PowerShell):**
```powershell
python -m venv venv
./venv/Scripts/Activate.ps1
```

**Windows (CMD):**
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Verify activation:**
- Your prompt should show `(venv)` prefix
- Run: `which python` (Linux/Mac) or `where python` (Windows)
- Should point to the venv directory

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Expected output:**
```
Successfully installed fastapi-0.109.0 uvicorn-0.27.0 pandas-2.2.0 ...
```

**Verify installation:**
```bash
pip list
```

### 4. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env if needed (optional)
# Default values are production-ready
```

**Default configuration:**
```env
SESSION_EXPIRY_HOURS=24
MAX_FILE_SIZE_MB=10
CLEANUP_INTERVAL_MINUTES=10
```

### 5. Run the Server

```bash
uvicorn main:app --reload
```

**Expected output:**
```
INFO:     Will watch for changes in these directories: ['/path/to/talking_bi']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [12345] using StatReload
INFO:     Started server process [12346]
INFO:     Waiting for application startup.
Session cleanup scheduler started
INFO:     Application startup complete.
```

### 6. Verify Installation

**Open browser:**
```
http://localhost:8000
```

**Expected response:**
```json
{
  "message": "Talking BI API - Phase 0A",
  "status": "running"
}
```

**Test health endpoint:**
```bash
curl http://localhost:8000/health
```

**Expected response:**
```json
{
  "status": "healthy"
}
```

---

## Running Tests

### 1. Navigate to Tests Directory

```bash
cd tests
```

### 2. Install Test Dependencies

```bash
pip install requests
```

### 3. Run Test Suite

```bash
python test_api.py
```

**Expected output:**
```
=== Testing Health Endpoint ===
Status: 200
✓ Health check passed

=== Testing Valid CSV Upload ===
Status: 200
✓ Valid upload test passed

=== Testing Invalid File Type ===
Status: 400
✓ Invalid file type test passed

=== Testing Empty CSV ===
Status: 400
✓ Empty CSV test passed

==================================================
✓ ALL TESTS PASSED!
==================================================
```

---

## Troubleshooting

### Issue: Virtual Environment Not Activating

**Windows PowerShell Execution Policy Error:**
```powershell
# Run as Administrator
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Alternative (Windows):**
```cmd
# Use CMD instead of PowerShell
venv\Scripts\activate.bat
```

### Issue: Module Not Found

**Symptom:**
```
ModuleNotFoundError: No module named 'fastapi'
```

**Solution:**
```bash
# Ensure venv is activated (check for (venv) prefix)
# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: Port Already in Use

**Symptom:**
```
ERROR: [Errno 48] Address already in use
```

**Solution:**
```bash
# Option 1: Use different port
uvicorn main:app --reload --port 8001

# Option 2: Kill process using port 8000
# Windows
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# Linux/Mac
lsof -ti:8000 | xargs kill -9
```

### Issue: Import Errors

**Symptom:**
```
ImportError: cannot import name 'X' from 'Y'
```

**Solution:**
```bash
# Ensure you're in the correct directory
cd talking_bi

# Run from project root
uvicorn main:app --reload
```

### Issue: CSV Upload Fails

**Symptom:**
```
{"detail": "Failed to parse CSV: ..."}
```

**Solutions:**
1. Check file encoding (should be UTF-8)
2. Verify CSV format (comma-separated)
3. Ensure file has headers
4. Check for special characters
5. Verify file size < 10MB

### Issue: Session Cleanup Not Working

**Symptom:**
No cleanup messages in logs

**Solution:**
- Cleanup runs every 10 minutes
- Wait for the interval
- Check logs for "Cleaned up X expired session(s)"
- Verify APScheduler is installed: `pip show apscheduler`

---

## Development Setup

### IDE Configuration

**VS Code:**

1. Install Python extension
2. Select interpreter: `Ctrl+Shift+P` → "Python: Select Interpreter" → Choose venv
3. Create `.vscode/settings.json`:
```json
{
  "python.defaultInterpreterPath": "./venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.formatting.provider": "black"
}
```

**PyCharm:**

1. File → Settings → Project → Python Interpreter
2. Add Interpreter → Existing Environment
3. Select `venv/bin/python` (or `venv\Scripts\python.exe` on Windows)

### Hot Reload

The `--reload` flag enables automatic restart on code changes:
```bash
uvicorn main:app --reload
```

**Note:** Only use in development. Remove for production.

### Debug Mode

**VS Code launch.json:**
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "FastAPI",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": ["main:app", "--reload"],
      "jinja": true
    }
  ]
}
```

---

## Production Deployment

### Environment Variables

Create production `.env`:
```env
SESSION_EXPIRY_HOURS=24
MAX_FILE_SIZE_MB=10
CLEANUP_INTERVAL_MINUTES=10

# Add production-specific vars
# DATABASE_URL=postgresql://...
# API_KEY=...
```

### Run Without Reload

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Using Gunicorn (Recommended)

```bash
# Install gunicorn
pip install gunicorn

# Run with workers
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Docker Deployment

Create `Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:
```bash
docker build -t talking-bi .
docker run -p 8000:8000 talking-bi
```

---

## Next Steps

1. ✅ Complete setup and verify tests pass
2. 📖 Read [API_REFERENCE.md](API_REFERENCE.md) for endpoint details
3. 🧪 Experiment with uploading different CSV files
4. 🔧 Customize configuration in `.env`
5. 🚀 Prepare for Phase 0B (database persistence)

---

## Getting Help

- Check [README.md](../README.md) for overview
- Review [API_REFERENCE.md](API_REFERENCE.md) for API details
- Open an issue on GitHub
- Contact the development team

---

**Happy coding! 🎉**
