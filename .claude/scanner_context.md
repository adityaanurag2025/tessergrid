# Scanner Context

## What scanner.py Does
Scans an Excel/CSV file and detects all data quality problems.
Reports every problem with its exact row number, column letter, and column name.
Does NOT fix anything — detection only. Output feeds into cleaner.py.

Run from project root:
```
python src/scanner.py
python src/scanner.py data/input/myfile.xlsx
```

## Problems It Must Detect (11 total)

1. **Duplicate column names** — same name appears more than once (case-insensitive, ignoring extra whitespace)
2. **Mixed date formats** — same date column uses multiple formats (MM/DD/YYYY, YYYY-MM-DD, "Jan 20 2024", etc.)
3. **Inconsistent country names** — US vs USA vs United States vs U.S.A vs united states
4. **Inconsistent status values** — Delivered vs delivered vs DELIVERED
5. **Missing values** — empty cells
6. **Extra spaces in text** — leading, trailing, or double spaces inside a value
7. **Numbers stored as text** — "750" typed as a string instead of a number
8. **Duplicate rows** — two or more rows with identical business data
9. **Completely empty rows** — every cell in the row is blank
10. **Merged cells** — ERP exports often merge cells; breaks pandas processing
11. **Inconsistent supplier names** — Foxconn vs FOXCONN vs foxconn

## Expected Output Format

```
=====================================
    CHAINFIX - DATA SCAN REPORT
=====================================

FILE: data/samples/messy_supply_chain_data.xlsx
TOTAL ROWS: X
TOTAL COLUMNS: X

PROBLEMS FOUND:
---------------

1. DUPLICATE COLUMNS
   - "Order ID" (column A) and "order id" (column B) are the same column name

2. MISSING VALUES
   - Row 4, Column C (Customer Name): empty

3. MIXED DATE FORMATS
   - Row 3, Column D (Ship Date): "Jan 20 2024" — Month DD YYYY (long format)

... all problems listed

=====================================
  TOTAL PROBLEMS FOUND: X
=====================================
```

## Coding Rules for scanner.py
- Python and Pandas only (plus openpyxl for merged cells)
- Add a comment above every function explaining what it does
- Script must run from the project root folder
- Do not fix data — only detect and report
- Use exact Excel row numbers in output (row 1 = header, row 2 = first data row)
- Sample test file: `data/samples/messy_supply_chain_data.xlsx`

## Build Status
- [x] Session 1: scanner.py complete — detects all 9 active problem types
