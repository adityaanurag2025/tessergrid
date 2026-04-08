# Cleaner Context

## What cleaner.py Will Do
Takes the problem report produced by scanner.py and uses the Claude API
to intelligently fix each problem. Writes a cleaned Excel file to data/output/.
Also produces a fix summary (list of every change made) for reporter.py.

## How It Should Handle Each Problem Type

| Problem | Fix Strategy |
|---------|-------------|
| Duplicate columns | Drop the duplicate column, keep the first occurrence |
| Mixed date formats | Normalize all dates to ISO format: YYYY-MM-DD |
| Inconsistent country names | Standardize to full name: "United States" |
| Inconsistent status values | Standardize to title case: "Delivered", "In Transit", "Pending" |
| Missing values | Flag for review; do not guess — leave as NaN or use Claude to infer from context |
| Extra spaces | Strip leading/trailing whitespace; collapse internal double spaces |
| Numbers stored as text | Convert to numeric type (int or float as appropriate) |
| Duplicate rows | Remove all but the first occurrence |
| Empty rows | Drop completely |
| Merged cells | Unmerge and forward-fill the value into each previously merged cell |
| Inconsistent supplier names | Standardize to the most common casing in the column |

## Output Format

- **Cleaned file**: `data/output/<original_filename>_cleaned.xlsx`
- **Fix log**: list of dicts — one entry per change made:
  ```python
  {
      "row": 9,
      "column": "Quantity",
      "original": "750",
      "fixed": 750,
      "fix_type": "numbers_as_text"
  }
  ```

## API Usage
Read `.claude/api_context.md` before writing any API calls.

## Build Status
- [ ] Session 2: cleaner.py — not yet started
