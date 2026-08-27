# Implementation Summary: UTC and CC CSV Endpoints

## Changes Made

### 1. Modified `src/sanitize.py`

**Function: `create_clean_timestamps_csv`**

- Changed return type from `bytes` to `tuple[bytes, bytes, bytes]`
- Now returns `(et_csv_bytes, utc_csv_bytes, cc_csv_bytes)` instead of just `et_csv_bytes`
- Updated empty CSV cases to return tuple of 3 empty CSVs
- Files are still saved to disk as before, but the function now returns all 3 streams

### 2. Modified `src/download.py`

**New function: `_download_all()`**

- Returns tuple of `(clean_csv_bytes, utc_csv_bytes, cc_csv_bytes)`
- Handles both cache hit (reads all 3 files) and cache miss (unpacks tuple from `create_clean_timestamps_csv`)
- Centralizes logic for accessing all 3 CSV variants

**Modified function: `_download()`**

- Now calls `_download_all()` and returns only the clean CSV bytes
- Maintains backward compatibility with existing code

**New function: `get_utc_csv()`**

- Returns UTC CSV bytes by calling `_download_all()` and extracting the UTC bytes
- No longer reads from file - gets bytes directly from processing pipeline

**New function: `get_cc_csv()`**

- Returns CC (recent 6 months) CSV bytes by calling `_download_all()` and extracting the CC bytes
- No longer reads from file - gets bytes directly from processing pipeline

### 3. Modified `main.py`

**Imports:**

- Added `get_utc_csv` and `get_cc_csv` to imports from `src.download`

**MCP Tools:**

- Added `utc_csv_bytes()` - Returns UTC CSV as raw bytes
- Added `cc_csv_bytes()` - Returns CC CSV (recent 6 months) as raw bytes

**HTTP Endpoints:**

- Created handlers: `utc_csv` and `cc_csv` using `_make_stream_handler()`
- Added routes:
    - `GET /utc_csv` - Returns UTC timestamps CSV
    - `GET /cc_csv` - Returns recent 6 months ET timestamps CSV

## Benefits

1. **Memory Efficiency**: No need to read from files when data is already in memory
2. **Consistency**: All 3 CSV variants (clean, UTC, CC) are generated together and cached together
3. **Flexibility**: Can choose which CSV variant to use without multiple file I/O operations
4. **API Completeness**: Both UTC and CC CSV files are now accessible via HTTP endpoints and MCP tools
5. **Pattern Consistency**: Follows the same pattern as other endpoints in the application

## Usage

### MCP Tools

```python
# Get UTC CSV bytes
utc_data = utc_csv_bytes()

# Get CC CSV bytes (recent 6 months)
cc_data = cc_csv_bytes()
```

### HTTP Endpoints

```bash
# Get UTC CSV
curl http://localhost:8000/utc_csv

# Get CC CSV (recent 6 months)
curl http://localhost:8000/cc_csv
```

### Programmatic Access

```python
from src.download import get_utc_csv, get_cc_csv

utc_bytes = get_utc_csv()
cc_bytes = get_cc_csv()
```

