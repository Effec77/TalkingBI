# Contributing Guide

Thank you for considering contributing to Talking BI! This guide will help you get started.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)

---

## Code of Conduct

### Our Standards

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on constructive feedback
- Prioritize project goals over personal preferences

### Unacceptable Behavior

- Harassment or discrimination
- Trolling or insulting comments
- Publishing private information
- Unprofessional conduct

---

## Getting Started

### 1. Fork the Repository

```bash
# Click "Fork" on GitHub
# Clone your fork
git clone https://github.com/YOUR_USERNAME/talking_bi.git
cd talking_bi
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # Linux/Mac
./venv/Scripts/Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install pytest black pylint mypy
```

### 3. Create a Branch

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Or bugfix branch
git checkout -b bugfix/issue-description
```

---

## Development Workflow

### Branch Naming Convention

- `feature/` - New features
- `bugfix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Test additions/updates

**Examples:**
- `feature/add-pagination`
- `bugfix/fix-csv-parsing`
- `docs/update-api-reference`

### Commit Messages

Follow the conventional commits format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation
- `style` - Formatting
- `refactor` - Code restructuring
- `test` - Adding tests
- `chore` - Maintenance

**Examples:**
```
feat(api): add pagination to upload endpoint

- Add limit and offset parameters
- Update response format
- Add tests for pagination

Closes #123
```

```
fix(session): prevent memory leak in cleanup

- Fix reference counting issue
- Add proper garbage collection
- Update cleanup interval

Fixes #456
```

---

## Coding Standards

### Python Style Guide

Follow [PEP 8](https://pep8.org/) with these specifics:

**Formatting:**
- Line length: 88 characters (Black default)
- Indentation: 4 spaces
- Quotes: Double quotes for strings
- Trailing commas in multi-line structures

**Naming:**
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private: `_leading_underscore`

**Example:**
```python
from typing import Dict, List
import pandas as pd


class DataProcessor:
    """Process uploaded data files."""
    
    MAX_FILE_SIZE = 10_000_000  # 10MB
    
    def __init__(self, config: Dict):
        self.config = config
        self._cache = {}
    
    def process_file(self, file_path: str) -> pd.DataFrame:
        """
        Process a CSV file and return DataFrame.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Processed DataFrame
            
        Raises:
            ValueError: If file is invalid
        """
        # Implementation
        pass
```

### Type Hints

Always use type hints:

```python
# Good
def create_session(df: pd.DataFrame) -> str:
    pass

# Bad
def create_session(df):
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def calculate_statistics(data: pd.DataFrame, columns: List[str]) -> Dict:
    """
    Calculate statistics for specified columns.
    
    Args:
        data: Input DataFrame
        columns: List of column names to analyze
        
    Returns:
        Dictionary with statistics per column
        
    Raises:
        KeyError: If column doesn't exist
        ValueError: If data is empty
        
    Example:
        >>> df = pd.DataFrame({'a': [1, 2, 3]})
        >>> calculate_statistics(df, ['a'])
        {'a': {'mean': 2.0, 'std': 1.0}}
    """
    pass
```

### Error Handling

Be specific with exceptions:

```python
# Good
try:
    df = pd.read_csv(file_path)
except pd.errors.EmptyDataError:
    raise ValueError("CSV file is empty")
except pd.errors.ParserError as e:
    raise ValueError(f"Failed to parse CSV: {e}")

# Bad
try:
    df = pd.read_csv(file_path)
except Exception as e:
    raise Exception(f"Error: {e}")
```

---

## Testing Guidelines

### Test Structure

```python
"""
Test module for upload functionality.
"""
import pytest
from fastapi.testclient import TestClient


class TestUploadEndpoint:
    """Tests for CSV upload endpoint."""
    
    def test_valid_upload(self, client: TestClient):
        """Test successful CSV upload."""
        # Arrange
        files = {"file": ("test.csv", b"col1,col2\n1,2", "text/csv")}
        
        # Act
        response = client.post("/upload", files=files)
        
        # Assert
        assert response.status_code == 200
        assert "session_id" in response.json()
    
    def test_invalid_file_type(self, client: TestClient):
        """Test rejection of non-CSV files."""
        files = {"file": ("test.txt", b"not a csv", "text/plain")}
        response = client.post("/upload", files=files)
        assert response.status_code == 400
```

### Running Tests

```bash
# Run all tests
python tests/test_api.py

# With pytest (if installed)
pytest tests/

# With coverage
pytest --cov=. tests/
```

### Test Coverage

Aim for:
- 80%+ overall coverage
- 100% for critical paths
- All error cases tested

---

## Documentation

### When to Update Docs

Update documentation when:
- Adding new endpoints
- Changing API behavior
- Adding configuration options
- Fixing bugs that affect usage

### Documentation Files

| File | Purpose | Update When |
|------|---------|-------------|
| `README.md` | Project overview | Major changes |
| `docs/API_REFERENCE.md` | API details | New endpoints |
| `docs/SETUP_GUIDE.md` | Installation | Setup changes |
| `docs/PROJECT_STRUCTURE.md` | Architecture | Structure changes |

### Documentation Style

- Use clear, concise language
- Include code examples
- Add diagrams for complex flows
- Keep formatting consistent

---

## Submitting Changes

### Pre-Submission Checklist

- [ ] Code follows style guide
- [ ] All tests pass
- [ ] New tests added for new features
- [ ] Documentation updated
- [ ] No sensitive data in commits
- [ ] `.env` not committed
- [ ] Commit messages follow convention

### Pull Request Process

1. **Update your branch**
```bash
git fetch upstream
git rebase upstream/main
```

2. **Run tests**
```bash
python tests/test_api.py
```

3. **Format code**
```bash
black .
pylint **/*.py
```

4. **Push to your fork**
```bash
git push origin feature/your-feature
```

5. **Create Pull Request**
- Go to GitHub
- Click "New Pull Request"
- Fill out the template
- Link related issues

### Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Refactoring

## Testing
- [ ] All tests pass
- [ ] New tests added
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guide
- [ ] Documentation updated
- [ ] No breaking changes
- [ ] Backward compatible

## Related Issues
Closes #123
```

### Review Process

1. Automated checks run (if configured)
2. Maintainer reviews code
3. Feedback provided
4. Changes requested (if needed)
5. Approval and merge

---

## Project Organization Rules

### File Organization

**✅ DO:**
- Keep all docs in `docs/`
- Keep all tests in `tests/`
- Keep all data in `data/`
- Use meaningful file names

**❌ DON'T:**
- Leave files in root directory
- Create random test files
- Commit temporary files
- Mix concerns in files

### Security

**✅ DO:**
- Use `.env` for secrets
- Commit `.env.example`
- Add sensitive patterns to `.gitignore`
- Review code for security issues

**❌ DON'T:**
- Commit API keys
- Commit passwords
- Commit `.env` files
- Hardcode secrets

### Clean Code

**✅ DO:**
- Remove unused imports
- Delete commented code
- Clean up debug prints
- Remove temporary files

**❌ DON'T:**
- Leave debug code
- Keep unused functions
- Commit commented code
- Leave TODO comments without issues

---

## Getting Help

### Resources

- **Documentation**: Check `docs/` folder
- **Issues**: Search existing issues on GitHub
- **Discussions**: Use GitHub Discussions
- **Email**: Contact maintainers

### Asking Questions

When asking for help:
1. Search existing issues first
2. Provide context and details
3. Include error messages
4. Share relevant code snippets
5. Describe what you've tried

### Reporting Bugs

Use this template:

```markdown
**Describe the bug**
Clear description of the issue

**To Reproduce**
Steps to reproduce:
1. Go to '...'
2. Click on '...'
3. See error

**Expected behavior**
What should happen

**Actual behavior**
What actually happens

**Environment**
- OS: [e.g., Windows 10]
- Python: [e.g., 3.12]
- Version: [e.g., 0.1.0]

**Additional context**
Any other relevant information
```

---

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Credited in documentation

---

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT License).

---

**Thank you for contributing to Talking BI! 🎉**
