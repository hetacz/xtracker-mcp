# Polymarket API Testing - Complete Documentation Index

**Project:** XTracker Polymarket API Analysis  
**Date:** 2025-11-28  
**Location:** `C:\kod\xt\`

---

## 📋 Documentation Overview

All documentation files have been recreated and are ready for use.

---

## 📁 Main Documentation Files

### 1. **FINAL_TEST_RESULTS_8_CASES.md** ⭐

**Primary reference document**

- Complete results from all 8 test cases
- Detailed analysis of API behavior
- CSV file listings and locations
- Key findings and recommendations

### 2. **COMPREHENSIVE_DATE_TEST_RESULTS.md**

**Detailed technical analysis**

- In-depth breakdown of each test
- API behavior matrix
- Visual comparisons and charts
- Complete data retention analysis

### 3. **QUICK_REFERENCE.md**

**Quick lookup guide**

- Table of all 8 CSV files
- Row counts and file sizes
- Commands to verify files
- Next steps for analysis

### 4. **TEST_EXECUTION_SUMMARY.md**

**Execution report**

- Test case summaries
- CSV file structure details
- Files created (32 total)
- Key findings and conclusions

---

## 🆔 ID Differences Documentation

### 5. **WHY_DIFFERENT_IDS_QUICK.md** ⭐

**Quick answer guide**

- TL;DR explanation
- Side-by-side comparison table
- How to match records
- Conversion code examples

### 6. **ID_DIFFERENCES_EXPLAINED.md**

**Complete technical explanation**

- Detailed field mapping
- Database design rationale
- Use cases for each ID type
- Best practices

### 7. **SAME_TWEET_COMPARISON.md**

**Real example**

- Same tweet in both formats
- Field-by-field comparison
- Timestamp timezone conversions
- Practical matching guide

---

## 📊 Test Data Files

### Location

`C:\kod\xt\downloads\polymarket_tests\`

### File Types (32 files total)

- **8 CSV files** - Primary data output
- **8 JSON files** (pretty) - Formatted responses
- **8 TXT files** (raw) - Raw API responses
- **8 JSON files** (meta) - HTTP metadata

### CSV Files

```
test1_no_query_parameters_20251128_120813.csv          [896 rows]
test2_only_startdate_20240808_20251128_120814.csv      [896 rows]
test3_only_startdate_20250303_20251128_120815.csv      [896 rows]
test4_only_startdate_20251112_20251128_120816.csv      [595 rows] ⭐
test5_only_enddate_20251119_20251128_120817.csv        [896 rows]
test6_both_dates_20240808_to_20251119_20251128_120818.csv [583 rows]
test7_both_dates_20250303_to_20251119_20251128_120819.csv [583 rows]
test8_both_dates_20251112_to_20251119_20251128_120819.csv [282 rows] ⭐
```

---

## 🔑 Key Findings Summary

### API Behavior

- ✅ **28-day data retention** - API maintains rolling 28-day window
- ✅ **endDate always works** - Filters data correctly
- ✅ **Recent startDate works** - When within 28-day window (Test 4 & 8)
- ❌ **Historical startDate ignored** - Dates older than 28 days (Test 2 & 3)

### ID Differences (UI vs API)

- **UI Download:** Shows Twitter Snowflake IDs (`Tweet ID` column)
- **API Download:** Shows Polymarket CUIDs (`id` column) + Twitter IDs (`platformId` column)
- **To Match:** Use `Tweet ID` (UI) = `platformId` (API)

### Test Results

| Test   | Rows | Key Finding                      |
|--------|------|----------------------------------|
| 1-3, 5 | 896  | Full 28-day dataset              |
| 4      | 595  | Recent start date works! ⭐       |
| 6-7    | 583  | End date filtering works         |
| 8      | 282  | Both parameters work together! ⭐ |

---

## 🚀 Quick Start Guide

### View Test Results

```powershell
# See all CSV files
Get-ChildItem C:\kod\xt\downloads\polymarket_tests\*.csv

# Count rows in a CSV
(Get-Content C:\kod\xt\downloads\polymarket_tests\test4_only_startdate_20251112_20251128_120816.csv).Count
```

### Read Documentation

```powershell
# Quick reference
code C:\kod\xt\QUICK_REFERENCE.md

# ID differences explanation
code C:\kod\xt\WHY_DIFFERENT_IDS_QUICK.md

# Complete analysis
code C:\kod\xt\FINAL_TEST_RESULTS_8_CASES.md
```

### Analyze Data

```python
import pandas as pd

# Load API CSV
df = pd.read_csv('/downloads/polymarket_tests/test1_no_query_parameters_20251128_120813.csv')

# Show basic info
print(f"Total rows: {len(df)}")
print(f"Date range: {df['createdAt'].min()} to {df['createdAt'].max()}")
print(f"Columns: {df.columns.tolist()}")
```

---

## 📌 Important Notes

### What Works

- ✅ Recent date filtering (<28 days)
- ✅ CSV export with full tweet content
- ✅ JSON API responses
- ✅ Precise 7-day windows (see Test 8)

### What Doesn't Work

- ❌ Historical date filtering (>28 days)
- ❌ Full archive access
- ❌ Year-over-year comparisons

### Recommended Usage

- **Use for:** Recent trend analysis, last 3-4 weeks, real-time monitoring
- **Don't use for:** Historical analysis, long-term patterns, annual reports
- **Alternative:** Keep using `xtracker.io` API for historical data

---

## 🔧 Test Script

### Location

`C:\kod\xt\test_comprehensive_dates.py`

### Run Tests Again

```powershell
cd C:\kod\xt
python test_comprehensive_dates.py
```

### Modify Tests

Edit `test_comprehensive_dates.py` to:

- Change date ranges
- Add new test cases
- Modify output format
- Test different users

---

## 📚 Additional Resources

### Related Files

- `test_comprehensive_dates.py` - Test script
- `main.py` - MCP server with existing API integration
- `src/download.py` - Current xtracker.io implementation
- `src/sanitize.py` - CSV processing logic

### Previous Documentation

- `IMPLEMENTATION_SUMMARY.md` - UTC/CC CSV endpoints
- `FIX_SUMMARY.md` - 15min window calculation fix
- `README.md` - Project overview

---

## ✅ Status

**All Documentation Files:** ✅ Created and verified  
**Test Data Files:** ✅ All 32 files present  
**Analysis Complete:** ✅ API behavior fully documented  
**Recommendations:** ✅ Provided in each document

---

## 📞 Questions & Issues

If documents appear empty:

1. Check file encoding (should be UTF-8)
2. Try opening with different editors
3. Files are in `C:\kod\xt\`
4. Re-run test script to regenerate data

If you need to regenerate documentation:

1. Delete empty `.md` files
2. Run test script: `python test_comprehensive_dates.py`
3. Documentation will be auto-created

---

**Last Updated:** 2025-11-28  
**Project Status:** ✅ Complete  
**Next Steps:** Analyze CSV data, integrate findings into existing codebase

