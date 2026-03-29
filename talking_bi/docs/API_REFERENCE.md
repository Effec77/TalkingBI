# API Reference

## Base URL
```
http://localhost:8000
```

## Authentication
Currently, no authentication is required for Phase 0A.

---

## Endpoints

### 1. Root Endpoint

**GET** `/`

Returns basic API information.

**Response:**
```json
{
  "message": "Talking BI API - Phase 0A",
  "status": "running"
}
```

---

### 2. Health Check

**GET** `/health`

Check if the API is running and healthy.

**Response:**
```json
{
  "status": "healthy"
}
```

**Status Codes:**
- `200 OK` - Service is healthy

---

### 3. Upload CSV

**POST** `/upload`

Upload a CSV file and create a new session.

**Request:**

Headers:
```
Content-Type: multipart/form-data
```

Body:
- `file` (required): CSV file to upload

**cURL Example:**
```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@path/to/your/file.csv"
```

**Python Example:**
```python
import requests

with open("data.csv", "rb") as f:
    files = {"file": ("data.csv", f, "text/csv")}
    response = requests.post("http://localhost:8000/upload", files=files)
    
print(response.json())
```

**Success Response (200 OK):**
```json
{
  "session_id": "653a75a6-8f54-41da-9dcd-8ec59bb7c074",
  "dataset": {
    "filename": "sales_data.csv",
    "shape": [1000, 10],
    "columns": [
      "date",
      "sales",
      "region",
      "product",
      "quantity",
      "price",
      "customer_id",
      "category",
      "discount",
      "profit"
    ],
    "missing_pct": {
      "date": 0.0,
      "sales": 0.02,
      "region": 0.0,
      "product": 0.0,
      "quantity": 0.01,
      "price": 0.0,
      "customer_id": 0.05,
      "category": 0.0,
      "discount": 0.03,
      "profit": 0.02
    }
  }
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string | UUID for the created session |
| `dataset.filename` | string | Original filename |
| `dataset.shape` | array | [rows, columns] |
| `dataset.columns` | array | List of column names (normalized) |
| `dataset.missing_pct` | object | Missing value percentage per column |

**Error Responses:**

#### 400 Bad Request - Invalid File Type
```json
{
  "detail": "Invalid file type. Only .csv files are allowed."
}
```

#### 400 Bad Request - Empty CSV
```json
{
  "detail": "CSV file contains no data."
}
```

#### 400 Bad Request - Parse Error
```json
{
  "detail": "Failed to parse CSV: <error details>"
}
```

#### 413 Payload Too Large
```json
{
  "detail": "File size exceeds maximum allowed size of 10MB."
}
```

#### 500 Internal Server Error
```json
{
  "detail": "Failed to read file: <error details>"
}
```

---

## Data Processing

### Column Normalization

All column names are automatically normalized:
- Converted to lowercase
- Leading/trailing whitespace removed

**Example:**
```
Input:  " Sales Amount ", "REGION", "Product Name "
Output: "sales amount", "region", "product name"
```

### Missing Value Detection

Missing values are detected using pandas `isna()` method:
- Empty cells
- `NaN` values
- `None` values

The `missing_pct` field shows the percentage of missing values per column (0.0 to 1.0).

### Sample Values

For each column, the first 3 unique non-null values are extracted (not included in the response but stored in session).

---

## Session Management

### Session Lifecycle

1. **Creation**: Session created on successful CSV upload
2. **Storage**: DataFrame stored in-memory with metadata
3. **Expiry**: Sessions expire after 24 hours (configurable)
4. **Cleanup**: Expired sessions removed every 10 minutes

### Session Data Structure

```python
{
    "df": pandas.DataFrame,      # The uploaded data
    "created_at": datetime,      # Creation timestamp
    "expires_at": datetime       # Expiry timestamp
}
```

### Retrieving Session Data

Currently, there is no endpoint to retrieve session data (Phase 0A focuses on upload only). Future phases will add:
- `GET /session/{session_id}` - Retrieve session metadata
- `GET /session/{session_id}/data` - Retrieve DataFrame
- `DELETE /session/{session_id}` - Manually delete session

---

## Rate Limiting

Currently, no rate limiting is implemented. For production deployment, consider:
- Request rate limits per IP
- File upload limits per user
- Concurrent upload restrictions

---

## Error Handling

All errors follow FastAPI's standard error format:

```json
{
  "detail": "Error message describing what went wrong"
}
```

HTTP status codes follow REST conventions:
- `200` - Success
- `400` - Client error (invalid input)
- `413` - Payload too large
- `500` - Server error

---

## Validation Rules

### File Validation

| Rule | Value | Error |
|------|-------|-------|
| Extension | `.csv` only | 400 - Invalid file type |
| Size | ≤ 10MB | 413 - Payload too large |
| Content | Must have data rows | 400 - Empty CSV |
| Format | Valid CSV structure | 400 - Parse error |

### CSV Requirements

- Must have at least one header row
- Must have at least one data row
- Columns can have any valid name
- Data types are auto-detected by pandas

---

## Performance Considerations

### File Size
- Maximum: 10MB (configurable via `MAX_FILE_SIZE_MB`)
- Recommended: < 5MB for optimal performance
- Large files may take longer to parse

### Memory Usage
- Each session stores the full DataFrame in memory
- Monitor memory usage with many concurrent sessions
- Consider implementing pagination for large datasets in future phases

### Concurrent Uploads
- FastAPI handles concurrent requests asynchronously
- No artificial limits on concurrent uploads
- System resources are the only constraint

---

## Future Enhancements

Planned for upcoming phases:
- Session retrieval endpoints
- Data querying capabilities
- Pagination for large datasets
- WebSocket support for real-time updates
- Authentication and authorization
- Rate limiting
- Data persistence (database storage)
