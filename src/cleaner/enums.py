# cleaner/enums.py
# Enum column standardization and country name normalization.
# Part of ChainFix - Supply Chain Data Cleaning Tool

import sys
import os
import re

# Add src/ to path so we can import scanner functions
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scanner import col_index_to_letter


# Canonical enum maps: lowercase stripped key → canonical value.
# Used by fix_enum_cols() to standardize status, unit, and currency columns.
STATUS_MAP = {
    # Delivered variants
    "delivered":           "Delivered",
    "delieverd":           "Delivered",
    "deliverd":            "Delivered",
    "deliver":             "Delivered",
    "processed":           "Delivered",
    "completed":           "Delivered",
    "complete":            "Delivered",
    "done":                "Delivered",
    # In Transit variants
    "in transit":          "In Transit",
    "in-transit":          "In Transit",
    "intransit":           "In Transit",
    "shipped":             "In Transit",
    "dispatched":          "In Transit",
    "out for delivery":    "In Transit",
    # Pending variants
    "pending":             "Pending",
    "processing":          "Pending",
    # On Hold variants
    "on hold":             "On Hold",
    "onhold":              "On Hold",
    "on-hold":             "On Hold",
    # Cancelled variants
    "cancelled":           "Cancelled",
    "canceled":            "Cancelled",
    "cncld":               "Cancelled",
    "cancel":              "Cancelled",
    "failed":              "Cancelled",
    "rejected":            "Cancelled",
    # Returned variants — distinct from Cancelled in supply chain
    "returned":            "Returned",
    "returnedd":           "Returned",
    # Partially Delivered variants
    "partially delivered": "Partially Delivered",
    "partial shipment":    "Partially Delivered",
    "partial":             "Partially Delivered",
    # Explicit null mappings — these become empty after normalize_nulls but
    # are listed here for defensive coverage. None means "clear to empty".
    "na":                  None,
    "n/a":                 None,
    "nan":                 None,
    "-":                   None,
    "unknown":             None,
    "tbd":                 None,
}

UNIT_MAP = {
    "kg":        "kg",
    "kgs":       "kg",
    "kilo":      "kg",
    "kilograms": "kg",
    "pcs":       "pcs",
    "pieces":    "pcs",
    "pc":        "pcs",
    "unit":      "pcs",
    "units":     "pcs",
    "nos":       "nos",
    "no":        "nos",
    "numbers":   "nos",
    "ltr":       "ltr",
    "litre":     "ltr",
    "litres":    "ltr",
    "lt":        "L",
    "l":         "L",
    "liters":    "L",
}

CURRENCY_MAP = {
    "usd":       "USD",
    "us dollar": "USD",
    "inr":       "INR",
    "rupee":     "INR",
    "rupees":    "INR",
    "rs":        "INR",
    "rs.":       "INR",
    "eur":       "EUR",
    "euro":      "EUR",
    "gbp":       "GBP",
}

REGION_MAP = {
    "north":       "North",
    "northern":    "North",
    "north zone":  "North",
    "east":        "East",
    "eastern":     "East",
    "east zone":   "East",
    "south":       "South",
    "southern":    "South",
    "south zone":  "South",
    "west":        "West",
    "western":     "West",
    "west zone":   "West",
    "central":     "Central",
    "centre":      "Central",
    "central zone":"Central",
}

TRANSPORT_MAP = {
    "air":       "Air",
    "flight":    "Air",
    "airfreight":"Air",
    "sea":       "Sea",
    "ship":      "Sea",
    "ocean":     "Sea",
    "road":      "Road",
    "truck":     "Road",
    "lorry":     "Road",
    "rail":      "Rail",
    "train":     "Rail",
    "courier":   "Courier",
    "express":   "Courier",
}

WAREHOUSE_MAP = {
    # Ahmedabad
    "ahmedabad wh":      "Ahmedabad WH",
    "ahmedabad  wh":     "Ahmedabad WH",
    # Bangalore
    "bangalore wh":      "Bangalore WH",
    "bangalore  wh":     "Bangalore WH",
    # BLR DC
    "blr - dc":          "BLR-DC",
    "blr-dc":            "BLR-DC",
    "blr dc":            "BLR-DC",
    # Chennai
    "chennai wh":        "Chennai WH",
    "chennai  wh":       "Chennai WH",
    # Delhi
    "delhi warehouse":   "Delhi Warehouse",
    "delhi  warehouse":  "Delhi Warehouse",
    # Hyderabad
    "hyderabad wh":      "Hyderabad WH",
    "hyderabad  wh":     "Hyderabad WH",
    # Jaipur
    "jaipur wh":         "Jaipur WH",
    "jaipur  wh":        "Jaipur WH",
    # Kolkata
    "kolkata wh":        "Kolkata WH",
    "kolkata  wh":       "Kolkata WH",
    # Lucknow
    "lucknow wh":        "Lucknow WH",
    "lucknow  wh":       "Lucknow WH",
    # Mumbai
    "mumbai wh":         "Mumbai WH",
    "mumbai_wh":         "Mumbai WH",
    # Nagpur
    "nagpur wh":         "Nagpur WH",
    "nagpur  wh":        "Nagpur WH",
    # Plant
    "plant 1":           "Plant 1",
    "plant  1":          "Plant 1",
    "plant 2":           "Plant 2",
    "plant  2":          "Plant 2",
    # Pune
    "pune wh":           "Pune WH",
    "pune  wh":          "Pune WH",
    # Vizag
    "vizag wh":          "Vizag WH",
    "vizag  wh":         "Vizag WH",
    # WH-07
    "wh - 07":           "WH-07",
    "wh-07":             "WH-07",
    "wh07":              "WH-07",
}

# Maps column name patterns to their canonical dictionary.
_ENUM_COL_MAPS = {
    "status":         STATUS_MAP,
    "unit":           UNIT_MAP,
    "currency":       CURRENCY_MAP,
    "region":         REGION_MAP,
    "transport mode": TRANSPORT_MAP,
    "transport_mode": TRANSPORT_MAP,
    "warehouse":      WAREHOUSE_MAP,
}


# Scans every column whose name matches a key in _ENUM_COL_MAPS and applies
# the canonical dictionary. Values found in the map are standardized.
# Values NOT in the map are kept as-is and returned as unknown_flags so they
# appear in the Flagged Items sheet as well as Fix Details.
# Returns: df, fix_log, unknown_flags
def fix_enum_cols(df):
    fix_log      = []
    unknown_flags = []
    cols = list(df.columns)
    _NULL_LIKE = {"", "nan", "none", "null", "na", "n/a", "-", "--"}
    for col_idx, col in enumerate(cols):
        col_key = col.strip().lower()
        mapping = _ENUM_COL_MAPS.get(col_key)
        if mapping is None:
            continue
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str.lower() in _NULL_LIKE:
                continue
            key = re.sub(r"\s+", " ", val_str.lower())
            excel_row = idx + 2
            if key in mapping:
                canonical = mapping[key]
                if canonical is None:
                    # Explicitly mapped to null
                    fix_log.append({
                        "fix_type": "inconsistent_text",
                        "row": excel_row,
                        "column": col,
                        "original": val_str,
                        "fixed": "",
                        "action": f"Null placeholder '{val_str}' in enum column — set to empty",
                        "fixed_by": "Rule-Based",
                    })
                    df.at[idx, col] = ""
                elif canonical != val_str:
                    fix_log.append({
                        "fix_type": "inconsistent_text",
                        "row": excel_row,
                        "column": col,
                        "original": val_str,
                        "fixed": canonical,
                        "action": f"Standardized '{val_str}' → '{canonical}'",
                        "fixed_by": "Rule-Based",
                    })
                    df.at[idx, col] = canonical
            else:
                # Unrecognized value — keep original, flag for review in both
                # Fix Details (via fix_log) and Flagged Items (via unknown_flags)
                fix_log.append({
                    "fix_type": "inconsistent_text",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": val_str,
                    "action": f"Unknown {col} value '{val_str}' — kept as-is, flagged for review",
                    "fixed_by": "Rule-Based",
                })
                unknown_flags.append({
                    "row":        excel_row,
                    "column":     col,
                    "col_letter": col_index_to_letter(col_idx),
                    "note":       f"Unknown {col} value '{val_str}' — review required",
                    "flag_type":  "unknown_enum",
                    "issue":      f"Unknown {col} value",
                    "original":   val_str,
                })
    return df, fix_log, unknown_flags


# Explicit mapping of all known country abbreviations/variants to full country names.
# This handles cases where variants normalize differently (e.g. "US" vs "United States")
# and would not be caught by the similarity-grouping approach in build_inconsistent_text_summary.
COUNTRY_CANONICAL = {
    "us": "United States",
    "usa": "United States",
    "u.s.a": "United States",
    "u.s.a.": "United States",
    "united states": "United States",
    "uk": "United Kingdom",
    "u.k": "United Kingdom",
    "gb": "United Kingdom",
    "cn": "China",
    "de": "Germany",
    "jp": "Japan",
    "kr": "South Korea",
    "south korea": "South Korea",
}


# Finds any column whose header contains "country" and applies COUNTRY_CANONICAL
# to standardize all abbreviations and lowercase variants to full country names.
# Returns the cleaned DataFrame and fix log entries.
def fix_country_names(df):
    fix_log = []
    for col in df.columns:
        if "country" not in col.lower():
            continue
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str == "" or val_str.lower() == "nan":
                continue
            canonical = COUNTRY_CANONICAL.get(val_str.lower())
            if canonical and canonical != val_str:
                excel_row = idx + 2
                fix_log.append({
                    "fix_type": "inconsistent_text",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": canonical,
                    "action": f"Standardized country '{val_str}' → '{canonical}'"
                })
                df.at[idx, col] = canonical
    return df, fix_log
