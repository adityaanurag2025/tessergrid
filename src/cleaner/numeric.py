# cleaner/numeric.py
# Numeric coercion and total value recalculation.
# Part of ChainFix - Supply Chain Data Cleaning Tool

import re


# Column name keywords that identify numeric columns.
# Only these columns are processed by fix_numbers_as_text.
_NUMERIC_COL_KEYWORDS = {"qty", "quantity", "price", "lead time", "lead_time", "total value", "total_value"}


# Coerces numeric columns to proper numbers.
# Targets only columns whose header matches _NUMERIC_COL_KEYWORDS.
# Rules:
#   - Null-like values (NULL, N/A, blank, "-") → empty string
#   - Comma-formatted numbers ("5,600.00") → strip commas then parse
#   - Valid numeric strings → int if whole, float otherwise
#   - Unparseable text (e.g. "abc", "five") → empty string + flagged
# Returns the cleaned DataFrame and fix log entries.
def fix_numbers_as_text(df):
    fix_log = []
    _NULL_LIKE = {"", "nan", "none", "null", "na", "n/a", "-", "--"}

    for col in df.columns:
        col_key = re.sub(r"\s+", " ", col.strip().lower())
        if not any(kw in col_key for kw in _NUMERIC_COL_KEYWORDS):
            continue

        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            excel_row = idx + 2

            # Normalize null-likes to empty
            if val_str.lower() in _NULL_LIKE:
                if val_str != "":
                    fix_log.append({
                        "fix_type": "numbers_fixed",
                        "row": excel_row,
                        "column": col,
                        "original": val_str,
                        "fixed": "",
                        "action": f"Null-like value '{val_str}' — set to empty",
                    })
                    df.at[idx, col] = ""
                continue

            # Strip commas, leading currency symbols, and trailing % before parse
            cleaned = val_str.replace(",", "")
            cleaned = cleaned.lstrip("$€£₹¥").rstrip("%")
            try:
                numeric = float(cleaned)
                converted = int(numeric) if numeric == int(numeric) else numeric
                result_str = str(converted)
                if result_str != val_str:
                    fix_log.append({
                        "fix_type": "numbers_fixed",
                        "row": excel_row,
                        "column": col,
                        "original": val_str,
                        "fixed": result_str,
                        "action": f"Converted text '{val_str}' to number {converted}",
                    })
                df.at[idx, col] = result_str
            except (ValueError, TypeError):
                # Unparseable — null out and flag
                fix_log.append({
                    "fix_type": "numbers_fixed",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": "",
                    "action": f"Could not parse '{val_str}' as number — set to empty, flagged for review",
                })
                df.at[idx, col] = ""

    return df, fix_log


# Recalculates the Total Value column as Quantity × Unit Price for every row.
# Finds the relevant columns by searching headers case-insensitively.
# Skips rows where either Quantity or Unit Price is missing.
# Returns the updated DataFrame and fix log entries.
def recalculate_total_value(df):
    fix_log = []
    qty_col = next((c for c in df.columns if "quantity" in c.lower()), None)
    price_col = next((c for c in df.columns if "unit price" in c.lower()), None)
    total_col = next((c for c in df.columns if "total value" in c.lower()), None)

    if not (qty_col and price_col and total_col):
        return df, fix_log

    for idx in df.index:
        qty_str = str(df.at[idx, qty_col]).strip()
        price_str = str(df.at[idx, price_col]).strip()
        if qty_str in ("", "nan") or price_str in ("", "nan"):
            continue
        try:
            total = float(qty_str) * float(price_str)
            result = int(total) if total == int(total) else round(total, 2)
            existing_str = str(df.at[idx, total_col]).strip()
            try:
                existing_float = float(existing_str)
            except (ValueError, TypeError):
                existing_float = None
            # Only log and update if value actually changed
            if existing_float is None or existing_float != float(result):
                fix_log.append({
                    "fix_type": "total_value_calculated",
                    "row": idx + 2,
                    "column": total_col,
                    "original": existing_str,
                    "fixed": str(result),
                    "action": f"Calculated {qty_str} × {price_str} = {result}"
                })
                df.at[idx, total_col] = str(result)
        except (ValueError, TypeError):
            pass
    return df, fix_log
