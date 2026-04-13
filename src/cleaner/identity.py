# cleaner/identity.py
# Whitespace cleanup and customer ID normalization.
# Part of Tessergrid - AI Data Cleaner

import re


# Strips leading and trailing whitespace from all text cells.
# Also collapses multiple consecutive internal spaces down to one.
# Returns the cleaned DataFrame and fix log entries for every cell changed.
def fix_whitespace(df):
    fix_log = []
    for col in df.columns:
        for idx, val in enumerate(df[col]):
            val_str = str(val)
            if val_str == "" or val_str.lower() == "nan":
                continue
            cleaned = re.sub(r"  +", " ", val_str.strip())
            if cleaned != val_str:
                excel_row = idx + 2
                fix_log.append({
                    "fix_type": "whitespace_cleaned",
                    "row": excel_row,
                    "column": col,
                    "original": repr(val_str),
                    "fixed": repr(cleaned),
                    "action": "Stripped leading/trailing/double whitespace"
                })
                df.at[idx, col] = cleaned
    return df, fix_log


# Normalizes customer_id values that follow the "cust-NNNN" pattern to uppercase.
# Example: "cust-1002" → "CUST-1002". Does not touch other ID formats (e.g. "C 1013").
# Returns the cleaned DataFrame and fix log entries.
def fix_customer_id(df):
    fix_log = []
    cust_col = next(
        (c for c in df.columns if "customer" in c.lower() and "id" in c.lower()), None
    )
    if cust_col is None:
        return df, fix_log
    pattern = re.compile(r"^cust-\d+$", re.IGNORECASE)
    for idx, val in enumerate(df[cust_col]):
        val_str = str(val).strip()
        if pattern.match(val_str) and val_str != val_str.upper():
            upper = val_str.upper()
            excel_row = idx + 2
            fix_log.append({
                "fix_type": "inconsistent_text",
                "row":      excel_row,
                "column":   cust_col,
                "original": val_str,
                "fixed":    upper,
                "action":   f"Normalized customer ID '{val_str}' → '{upper}'",
                "fixed_by": "Rule-Based",
            })
            df.at[idx, cust_col] = upper
    return df, fix_log
