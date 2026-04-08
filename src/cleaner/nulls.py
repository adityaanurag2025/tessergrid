# cleaner/nulls.py
# Null placeholder normalization.
# Part of ChainFix - Supply Chain Data Cleaning Tool

import pandas as pd


# Placeholder strings that represent missing data but are not actual empty cells.
# Applied as the very first pipeline step so all downstream logic sees "" for nulls.
_NULL_TOKENS = {
    "null", "none", "na", "n/a", "nan", "-", "--", "[empty]", "nil",
    "n.a.", "n.a", "missing", "tbd", "unknown", "not available",
    "not applicable", "#n/a", "#null!", "void", "empty",
}


# Converts all null-like placeholder tokens to empty string across every cell.
# Must run before any other fix so downstream steps only need to check for "".
# Returns the cleaned DataFrame and fix log entries.
def normalize_nulls(df):
    fix_log = []
    for col in df.columns:
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str != "" and val_str.lower() in _NULL_TOKENS:
                excel_row = idx + 2
                fix_log.append({
                    "fix_type": "whitespace_cleaned",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": "",
                    "action": f"Null placeholder '{val_str}' → empty",
                })
                df.at[idx, col] = ""
    return df, fix_log
