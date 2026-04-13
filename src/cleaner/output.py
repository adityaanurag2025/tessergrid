# cleaner/output.py
# File saving, summary printing, and the main clean_file() pipeline entry point.
# Part of Tessergrid - AI Data Cleaner

import sys
import os
from collections import defaultdict
from pathlib import Path

from .nulls      import normalize_nulls
from .structural import (
    unmerge_and_load, fix_duplicate_columns, fix_empty_rows,
    fix_erp_footer_rows, fix_duplicate_rows, fix_duplicate_by_business_key,
)
from .numeric    import fix_numbers_as_text, recalculate_total_value
from .dates      import fix_dates
from .enums      import fix_enum_cols, fix_country_names
from .text       import (
    build_inconsistent_text_summary, call_claude_for_mappings,
    majority_vote_fallback, apply_text_mappings,
)
from .identity   import fix_whitespace, fix_customer_id
from .flags      import flag_missing_values, apply_data_quality_flags


# Saves the cleaned DataFrame to an Excel file at the given output path.
# Creates the output directory if it does not already exist.
def save_clean_file(df, output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)


# Prints the final fix summary report to stdout.
def print_summary(input_file, output_file, fix_log, flags, unmerged_count):
    by_type = defaultdict(int)
    for entry in fix_log:
        by_type[entry["fix_type"]] += 1
    flags_count = len(flags)

    print()
    print("=====================================")
    print("   CHAINFIX - FIX SUMMARY REPORT     ")
    print("=====================================")
    print()
    print(f"ORIGINAL FILE: {os.path.basename(input_file)}")
    print(f"CLEAN FILE:    {os.path.basename(output_file)}")
    print()
    print("FIXES APPLIED:")
    print("--------------")

    label_map = {
        "duplicate_columns":      "DUPLICATE COLUMNS removed",
        "empty_rows":             "EMPTY ROWS removed",
        "erp_footer_rows":        "ERP FOOTER ROWS removed",
        "duplicate_rows":         "DUPLICATE ROWS removed",
        "whitespace_cleaned":     "WHITESPACE cells cleaned",
        "numbers_fixed":          "NUMBERS AS TEXT converted",
        "mixed_dates":            "MIXED DATES standardized to YYYY-MM-DD",
        "inconsistent_text":      "INCONSISTENT TEXT cells standardized",
        "total_value_calculated": "TOTAL VALUE cells recalculated",
    }

    counter = 1
    for fix_type, label in label_map.items():
        count = by_type.get(fix_type, 0)
        if fix_type == "merged_cells_fixed":
            count = unmerged_count
        print(f"{counter}. {label}: {count}")
        counter += 1
    print(f"{counter}. MERGED CELL RANGES unmerged: {unmerged_count}")

    total_fixes = sum(by_type.values()) + unmerged_count
    print()
    print("=====================================")
    print(f"  TOTAL FIXES APPLIED: {total_fixes}")
    print(f"  FLAGS FOR REVIEW (missing values): {flags_count}")
    print("=====================================")
    print()

    if flags_count > 0:
        print("MISSING VALUES FLAGGED FOR REVIEW:")
        print("-----------------------------------")
        for f in flags:
            print(f"  - Row {f['row']}, Column {f['col_letter']} ({f['column']}): empty")
        print()


# Main entry point.
# Runs the full cleaning pipeline: unmerge → fix → Claude API → save → report.
# original_filename: if provided, shown in the report instead of the saved filepath.
def clean_file(filepath, original_filename=None):
    print()
    print("=====================================")
    print("    CHAINFIX - DATA CLEANER          ")
    print("=====================================")
    print(f"\nLoading: {filepath}")

    # Step 1: Unmerge cells and load into pandas
    print("\n[1/10] Unmerging merged cells...")
    df, unmerged_count = unmerge_and_load(filepath)
    rows_before = len(df)
    print(f"       {unmerged_count} merged range(s) unmerged.")

    # Step 2: Normalize null placeholders (--, Null, [empty], N/A, etc.)
    print("[2/10] Normalizing null placeholders...")
    df, log_nulls = normalize_nulls(df)
    print(f"       {len(log_nulls)} null placeholder(s) cleared.")

    # Step 3: Drop duplicate columns
    print("[3/10] Removing duplicate columns...")
    df, log_dup_cols = fix_duplicate_columns(df)
    print(f"       {len(log_dup_cols)} duplicate column(s) removed.")

    # Step 4: Drop empty rows and ERP footer rows
    print("[4/10] Removing empty and ERP footer rows...")
    df, log_empty = fix_empty_rows(df)
    df, log_footer = fix_erp_footer_rows(df)
    print(f"       {len(log_empty)} empty row(s) removed, {len(log_footer)} ERP footer row(s) removed.")

    # Step 5: Strip whitespace
    print("[5/10] Cleaning whitespace...")
    df, log_ws = fix_whitespace(df)
    print(f"       {len(log_ws)} cell(s) cleaned.")

    # Step 5b: Normalize customer IDs (cust-NNNN → CUST-NNNN)
    print("[5b]   Normalizing customer IDs...")
    df, log_cust = fix_customer_id(df)
    print(f"       {len(log_cust)} customer ID(s) normalized.")

    # Step 6: Convert numbers stored as text
    print("[6/10] Converting numbers stored as text...")
    df, log_nums = fix_numbers_as_text(df)
    print(f"       {len(log_nums)} cell(s) converted.")

    # Step 7: Standardize date formats
    print("[7/10] Standardizing date formats...")
    df, log_dates = fix_dates(df)
    print(f"       {len(log_dates)} date cell(s) standardized.")

    # Step 8a: Standardize enum columns (status, unit, currency, region, transport mode)
    print("[8a]   Standardizing enum columns...")
    df, log_enums, enum_flags = fix_enum_cols(df)
    print(f"       {len(log_enums)} enum value(s) standardized, {len(enum_flags)} unknown value(s) flagged.")

    # Step 8b: Standardize country names with explicit mapping table
    print("[8b]   Standardizing country names...")
    df, log_countries = fix_country_names(df)
    print(f"       {len(log_countries)} country value(s) standardized.")

    # Step 8c: Standardize remaining inconsistent text via Claude API
    print("[8c]   Standardizing remaining inconsistent text via Claude API...")
    text_summary = build_inconsistent_text_summary(df)

    api_response = call_claude_for_mappings(text_summary)

    if api_response and "mappings" in api_response:
        mappings = api_response["mappings"]
        fixed_by = "Claude AI"
        print(f"       Claude API returned mappings for {len(mappings)} column(s).")
    else:
        print("       Using majority-vote fallback for text standardization.")
        mappings = majority_vote_fallback(df, text_summary)
        fixed_by = "Rule-Based"

    df, log_text = apply_text_mappings(df, mappings, fixed_by=fixed_by)
    print(f"       {len(log_text)} cell(s) standardized.")

    # Step 9: Deduplicate by business key AFTER full normalization
    # Key: order_id, sku, supplier, warehouse, order_date, qty_ordered, qty_shipped
    print("[9/10] Removing duplicate rows by business key (post-normalization)...")
    df, log_dupes = fix_duplicate_rows(df)
    df, log_biz_dupes = fix_duplicate_by_business_key(df)
    print(f"       {len(log_dupes)} exact duplicate(s) removed, {len(log_biz_dupes)} business-key duplicate(s) removed.")

    # Step 10: Recalculate Total Value = Quantity × Unit Price
    print("[10/10] Recalculating Total Value column...")
    df, log_totals = recalculate_total_value(df)
    print(f"        {len(log_totals)} Total Value cell(s) calculated.")

    # Step 11: Apply business rule validation — writes data_quality_flag column
    print("[11/11] Applying business rule validation...")
    df, quality_flags = apply_data_quality_flags(df)
    flagged_rows = sum(1 for r in df["data_quality_flag"] if r != "OK")
    print(f"        {flagged_rows} row(s) flagged with business rule issues.")

    # Collect missing value flags, business rule flags, and unknown enum flags
    flags = flag_missing_values(df) + quality_flags + enum_flags

    # Build combined fix log
    all_fixes = (
        log_nulls + log_dup_cols + log_empty + log_footer +
        log_ws + log_cust + log_nums + log_dates + log_enums + log_countries + log_text +
        log_dupes + log_biz_dupes + log_totals
    )

    # Save clean file
    output_path = "data/output/clean_supply_chain_data.xlsx"
    save_clean_file(df, output_path)
    print(f"\nClean file saved → {output_path}")

    # Print summary
    print_summary(filepath, output_path, all_fixes, flags, unmerged_count)

    return {
        "input_file":        original_filename or filepath,
        "output_file":       output_path,
        "rows_before":       rows_before,
        "rows_after":        len(df),
        "fix_log":           all_fixes,
        "flags":             flags,
        "unmerged_count":    unmerged_count,
    }


if __name__ == "__main__":
    import sys as _sys
    filepath = "data/samples/messy_supply_chain_data.xlsx"
    if len(_sys.argv) > 1:
        filepath = _sys.argv[1]
    clean_file(filepath)
