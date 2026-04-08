# cleaner/flags.py
# Missing value flags and business rule validation.
# Part of ChainFix - Supply Chain Data Cleaning Tool

import sys
import os

# Add src/ to path so we can import scanner functions
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scanner import col_index_to_letter


# Scans the cleaned DataFrame for remaining missing values.
# Does NOT modify the data — only adds entries to the flags list for the report.
def flag_missing_values(df):
    flags = []
    for col_idx, col in enumerate(df.columns):
        col_letter = col_index_to_letter(col_idx)
        for idx, val in enumerate(df[col]):
            if str(val).strip() == "" or str(val).strip().lower() == "nan":
                excel_row = idx + 2
                flags.append({
                    "row": excel_row,
                    "column": col,
                    "col_letter": col_letter,
                    "note": "Missing value — flagged for review"
                })
    return flags


# Applies business rule validation to the DataFrame.
# Adds a 'data_quality_flag' column: "OK" by default, or pipe-joined issue descriptions.
# Rules checked:
#   - ship_date < order_date
#   - delivery_date < ship_date
#   - qty_shipped > qty_ordered
#   - qty_ordered < 0
#   - lead_time_days < 0
#   - expiry_date < delivery_date
# Also returns a list of flag dicts for the reporter (one per issue per row).
def apply_data_quality_flags(df):
    flags = []
    cols  = list(df.columns)

    def find_col(*keywords):
        for c in cols:
            cl = c.lower()
            if all(kw in cl for kw in keywords):
                return c
        return None

    order_date_col    = find_col("order", "date")
    ship_date_col     = find_col("ship", "date")
    delivery_date_col = find_col("deliver", "date")
    expiry_date_col   = find_col("expir")
    qty_ordered_col   = find_col("qty", "order") or find_col("quantity", "order")
    qty_shipped_col   = find_col("qty", "ship") or find_col("quantity", "ship")
    lead_time_col     = find_col("lead")

    _EMPTY = {"", "nan", "none"}

    # Initialize flag column
    row_issues = ["OK"] * len(df)

    def add_issue(idx, issue_str, col, original):
        if row_issues[idx] == "OK":
            row_issues[idx] = issue_str
        else:
            row_issues[idx] += " | " + issue_str
        excel_row = idx + 2
        flags.append({
            "row":        excel_row,
            "column":     col,
            "col_letter": col_index_to_letter(cols.index(col)) if col in cols else "?",
            "note":       issue_str,
            "flag_type":  "business_rule",
            "issue":      issue_str,
            "original":   original,
        })

    for idx in df.index:
        od = str(df.at[idx, order_date_col]).strip()    if order_date_col    else ""
        sd = str(df.at[idx, ship_date_col]).strip()     if ship_date_col     else ""
        dd = str(df.at[idx, delivery_date_col]).strip() if delivery_date_col else ""
        ed = str(df.at[idx, expiry_date_col]).strip()   if expiry_date_col   else ""

        # ship_date < order_date
        if (od not in _EMPTY and od.lower() not in _EMPTY
                and sd not in _EMPTY and sd.lower() not in _EMPTY):
            try:
                if sd < od:
                    add_issue(idx, "ship_date before order_date", ship_date_col, sd)
            except Exception:
                pass

        # delivery_date < ship_date
        if (sd not in _EMPTY and sd.lower() not in _EMPTY
                and dd not in _EMPTY and dd.lower() not in _EMPTY):
            try:
                if dd < sd:
                    add_issue(idx, "delivery_date before ship_date", delivery_date_col, dd)
            except Exception:
                pass

        # expiry_date < delivery_date
        if (dd not in _EMPTY and dd.lower() not in _EMPTY
                and ed not in _EMPTY and ed.lower() not in _EMPTY):
            try:
                if ed < dd:
                    add_issue(idx, "expiry_date before delivery_date", expiry_date_col, ed)
            except Exception:
                pass

        # qty checks
        try:
            ordered = float(str(df.at[idx, qty_ordered_col]).strip()) if qty_ordered_col else None
        except (ValueError, TypeError):
            ordered = None
        try:
            shipped = float(str(df.at[idx, qty_shipped_col]).strip()) if qty_shipped_col else None
        except (ValueError, TypeError):
            shipped = None

        if ordered is not None and ordered < 0:
            add_issue(idx, "qty_ordered negative", qty_ordered_col, str(ordered))
        if ordered is not None and ordered == 0:
            add_issue(idx, "qty_ordered is zero", qty_ordered_col, str(ordered))
        if shipped is not None and shipped < 0:
            add_issue(idx, "qty_shipped negative", qty_shipped_col, str(shipped))
        if shipped is not None and ordered is not None and shipped > ordered:
            add_issue(idx, "qty_shipped exceeds qty_ordered", qty_shipped_col, str(shipped))

        # lead_time_days < 0
        if lead_time_col:
            try:
                lt = float(str(df.at[idx, lead_time_col]).strip())
                if lt < 0:
                    add_issue(idx, "lead_time_days negative", lead_time_col, str(lt))
            except (ValueError, TypeError):
                pass

    df["data_quality_flag"] = row_issues
    return df, flags


# Kept for backwards compatibility — delegates to apply_data_quality_flags.
def add_quality_flags(df):
    _, flags = apply_data_quality_flags(df.copy())
    return flags
