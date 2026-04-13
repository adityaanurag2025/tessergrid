# cleaner/dates.py
# Date format normalization.
# Part of Tessergrid - AI Data Cleaner

import re
import datetime


# Strict ordered format list — no fuzzy parsing.
# 2-digit year formats are tried last and get year correction in _parse_date_strict.
_DATE_FORMATS = [
    "%Y-%m-%d",   # 2026-01-15
    "%Y/%m/%d",   # 2026/01/15
    "%Y.%m.%d",   # 2026.01.15
    "%d-%m-%Y",   # 15-01-2026
    "%d/%m/%Y",   # 15/01/2026
    "%d %b %Y",   # 15 Jan 2026
    "%d %B %Y",   # 15 January 2026
    "%b %d %Y",   # Jan 15 2026
    "%B %d %Y",   # January 15 2026
    "%d/%m/%y",   # 15/01/26  (2-digit year — corrected below)
]
_TWO_DIGIT_YEAR_FMTS = {"%d/%m/%y"}


# Tries each format in _DATE_FORMATS in order, returns ISO string or None.
# 2-digit year rule: 00-30 → 2000-2030, 31-99 → 1931-1999.
def _parse_date_strict(raw):
    s = str(raw).strip()
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.datetime.strptime(s, fmt).date()
            if fmt in _TWO_DIGIT_YEAR_FMTS:
                yy = parsed.year % 100
                if yy > 30:
                    parsed = parsed.replace(year=1900 + yy)
            if parsed.year < 1900 or parsed.year > 2100:
                return None
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# Normalizes all date values in columns whose header contains "date" to YYYY-MM-DD.
# Uses a strict ordered format list — no fuzzy parsing.
# Note: ISO_PATTERN check removed intentionally — "2026-13-07" matches the pattern
# but is an impossible date. All values must be validated, not just non-ISO ones.
# Unparseable or impossible values are set to "" and logged for review.
# Returns the cleaned DataFrame and fix log entries.
def fix_dates(df):
    fix_log = []
    NULL_LIKE = {"", "nan", "none", "null", "na", "n/a", "-", "--"}
    for col in df.columns:
        if "date" not in col.lower():
            continue
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str.lower() in NULL_LIKE:
                continue
            iso_date = _parse_date_strict(val_str)
            excel_row = idx + 2
            if iso_date and iso_date == val_str:
                continue  # already a valid ISO date — no change needed
            if iso_date:
                fix_log.append({
                    "fix_type": "mixed_dates",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": iso_date,
                    "action": "Standardized date to YYYY-MM-DD",
                })
                df.at[idx, col] = iso_date
            else:
                fix_log.append({
                    "fix_type": "mixed_dates",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": "",
                    "action": "Could not parse date — set to empty, flagged for review",
                })
                df.at[idx, col] = ""
    return df, fix_log
