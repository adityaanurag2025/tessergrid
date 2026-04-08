# cleaner/text.py
# Inconsistent text detection and Claude API-based standardization.
# Part of ChainFix - Supply Chain Data Cleaning Tool

import os
import json
import time
import re
from collections import defaultdict


MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """You are a supply chain data cleaning expert specializing in Blue Yonder planning systems.

You will receive a list of data problems found in a supply chain Excel file. For each problem, decide the correct fix and return a JSON response.

Domain knowledge:
- INDDMDLINK: Independent Demand Link, alphanumeric, never treat as date or number
- Country names should be standardized to full name e.g. "United States" not "US" or "USA"
- Status values should be Title Case e.g. "Delivered" not "DELIVERED" or "delivered"
- Supplier names should be Title Case
- Dates should be standardized to YYYY-MM-DD format
- Empty rows should be removed
- Duplicate rows should be removed keeping first occurrence
- Merged cells should be unmerged and values copied to each cell
- Numbers stored as text should be converted to numbers
- Extra whitespace should be stripped from text fields

Return ONLY a JSON response in this exact format:
{
  "mappings": {
    "ColumnName": {
      "original_value": "canonical_value"
    }
  },
  "fixes": [
    {
      "problem_type": "inconsistent_text",
      "location": "Column L (Country)",
      "original_value": "US",
      "fixed_value": "United States",
      "action": "Standardized country name to full form"
    }
  ],
  "summary": {
    "total_fixes": 0,
    "by_type": {
      "mixed_dates": 0,
      "inconsistent_text": 0,
      "missing_values": 0,
      "duplicates_removed": 0,
      "empty_rows_removed": 0,
      "whitespace_cleaned": 0,
      "numbers_fixed": 0,
      "merged_cells_fixed": 0
    }
  }
}"""


# Scans all text columns for inconsistent values (same thing spelled differently).
# Returns a compact summary dict structured for the Claude API prompt.
# Format: {column_name: [list_of_variants]}
def build_inconsistent_text_summary(df):
    summary = {}
    for col in df.columns:
        value_groups = defaultdict(set)
        for val in df[col]:
            val_str = str(val).strip()
            if val_str == "" or val_str.lower() == "nan":
                continue
            normalized = re.sub(r"[^a-z0-9]", "", val_str.lower())
            value_groups[normalized].add(val_str)
        for normalized, variants in value_groups.items():
            if len(variants) > 1:
                if col not in summary:
                    summary[col] = []
                summary[col].append(sorted(variants))
    return summary


# Sends the inconsistent text groups to the Claude API.
# Returns the parsed JSON response containing canonical value mappings.
# Falls back to a majority-vote heuristic if the API call fails.
def call_claude_for_mappings(text_summary):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [INFO] ANTHROPIC_API_KEY not set — using majority-vote fallback for text standardization.")
        return None

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    user_message = (
        "Here are the inconsistent text groups found in a supply chain Excel file.\n"
        "For each group, decide the canonical (correct) form for each variant.\n\n"
        f"Inconsistent text groups:\n{json.dumps(text_summary, indent=2)}\n\n"
        "Return ONLY valid JSON with the mappings and fix entries as specified."
    )

    # Cap input size — prevent token bombing from huge files
    MAX_COLS   = 15
    MAX_GROUPS = 25
    text_summary = {
        col: groups[:MAX_GROUPS]
        for col, groups in list(text_summary.items())[:MAX_COLS]
    }

    print("  Calling Claude API for text standardization decisions...")
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
            timeout=60.0,
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if Claude wrapped the JSON
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except anthropic.RateLimitError:
        print("  [ERROR] Rate limit hit — waiting 15 seconds and retrying...")
        time.sleep(15)
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
                timeout=60.0,
            )
            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw)
        except Exception as e:
            print(f"  [ERROR] Retry also failed: {e}")
            return None
    except anthropic.APIError as e:
        print(f"  [ERROR] Claude API error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Could not parse Claude response as JSON: {e}")
        return None
    except Exception as e:
        print(f"  [ERROR] Unexpected error during Claude API call: {e}")
        return None


# Fallback when Claude API is unavailable.
# For each group of inconsistent variants, picks the most frequently appearing
# value in the DataFrame as the canonical form.
def majority_vote_fallback(df, text_summary):
    mappings = {}
    for col, groups in text_summary.items():
        if col not in df.columns:
            continue
        value_counts = df[col].value_counts()
        col_mappings = {}
        for variants in groups:
            # Find the most common variant in the actual data
            canonical = max(variants, key=lambda v: value_counts.get(v, 0))
            for v in variants:
                if v != canonical:
                    col_mappings[v] = canonical
        if col_mappings:
            mappings[col] = col_mappings
    return mappings


# Applies the canonical value mappings from Claude (or fallback) to the DataFrame.
# For each column in the mappings dict, replaces all variant values with the canonical form.
# fixed_by should be "Claude AI" or "Rule-Based" depending on who produced the mappings.
# Returns the cleaned DataFrame and fix log entries.
def apply_text_mappings(df, mappings, fixed_by="Rule-Based"):
    fix_log = []
    for col, col_mappings in mappings.items():
        if col not in df.columns:
            continue
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str in col_mappings:
                canonical = col_mappings[val_str]
                excel_row = idx + 2
                fix_log.append({
                    "fix_type": "inconsistent_text",
                    "row": excel_row,
                    "column": col,
                    "original": val_str,
                    "fixed": canonical,
                    "action": f"Standardized '{val_str}' → '{canonical}'",
                    "fixed_by": fixed_by,
                })
                df.at[idx, col] = canonical
    return df, fix_log
