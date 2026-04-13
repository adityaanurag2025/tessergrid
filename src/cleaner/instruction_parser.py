# cleaner/instruction_parser.py
# Parses free-text user instructions into structured rules via Claude API.
# Rules are then executed deterministically by apply_custom_rules().
# Part of Tessergrid - AI Data Cleaner

import os
import json
import time
import re

try:
    import anthropic
except ImportError:
    anthropic = None

MODEL = "claude-sonnet-4-20250514"

_SYSTEM_PROMPT = """You are a data cleaning rule parser for Tessergrid.

The user will describe in plain English how they want their spreadsheet cleaned.
You must convert their instruction into a structured JSON list of correction rules.
You NEVER see the data itself — only column names and the user's instruction.

Supported rule types:
- {"type": "rename_column", "from": "OldName", "to": "NewName"}
- {"type": "replace_value", "column": "ColName", "from": "old_value", "to": "new_value"}
- {"type": "drop_column", "column": "ColName"}

Rules:
- Only reference columns from the provided column list — never invent columns.
- If the instruction is ambiguous, destructive (delete all rows, etc.), or cannot be expressed
  in the supported rule types, return: {"rules": []}
- Return at most 5 rules.

Return ONLY valid JSON in this exact format:
{"rules": [ ... ]}"""


def parse_instruction(instruction: str, column_names: list, domain_key: str) -> list:
    """
    Send user instruction text to Claude to parse into structured rules.
    Returns a list of rule dicts. Returns [] if API is unavailable, instruction is empty,
    or parsing fails.
    """
    if not instruction or not instruction.strip():
        return []

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or anthropic is None:
        print("  [INFO] ANTHROPIC_API_KEY not set — skipping instruction parsing.")
        return []

    # Cap instruction length and column count
    instruction = instruction.strip()[:500]
    cols_to_send = column_names[:40]

    client = anthropic.Anthropic(api_key=api_key)

    user_message = (
        f"Domain: {domain_key}\n"
        f"Columns in this file: {json.dumps(cols_to_send)}\n\n"
        f"User instruction:\n{instruction}\n\n"
        "Convert to structured rules JSON."
    )

    print("  Calling Claude API to parse custom instructions...")
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
            timeout=30.0,
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        return parsed.get("rules", [])
    except anthropic.RateLimitError:
        print("  [ERROR] Rate limit hit — waiting 15 seconds and retrying...")
        time.sleep(15)
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
                timeout=30.0,
            )
            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            parsed = json.loads(raw)
            return parsed.get("rules", [])
        except Exception as e:
            print(f"  [ERROR] Retry failed: {e}")
            return []
    except anthropic.APIError as e:
        print(f"  [ERROR] Claude API error: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Could not parse Claude response as JSON: {e}")
        return []
    except Exception as e:
        print(f"  [ERROR] Unexpected error parsing instructions: {e}")
        return []


def apply_custom_rules(df, rules: list):
    """
    Apply structured correction rules to the DataFrame.
    Returns (df, fix_log).
    """
    fix_log = []
    cols = set(df.columns)

    for rule in rules:
        rule_type = rule.get("type")

        if rule_type == "rename_column":
            from_col = rule.get("from")
            to_col = rule.get("to")
            if not from_col or not to_col or from_col not in cols:
                continue
            df = df.rename(columns={from_col: to_col})
            cols = set(df.columns)
            fix_log.append({
                "fix_type": "rename_column",
                "row": "N/A",
                "column": from_col,
                "original": from_col,
                "fixed": to_col,
                "action": f"Renamed column '{from_col}' → '{to_col}'",
                "fixed_by": "User Instruction",
            })

        elif rule_type == "replace_value":
            col = rule.get("column")
            from_val = rule.get("from")
            to_val = rule.get("to")
            if not col or from_val is None or to_val is None or col not in cols:
                continue
            changed = 0
            for idx, val in enumerate(df[col]):
                if str(val).strip() == str(from_val).strip():
                    df.at[idx, col] = to_val
                    changed += 1
            if changed:
                fix_log.append({
                    "fix_type": "inconsistent_text",
                    "row": "multiple",
                    "column": col,
                    "original": from_val,
                    "fixed": to_val,
                    "action": f"Replaced '{from_val}' → '{to_val}' in '{col}' ({changed} cells)",
                    "fixed_by": "User Instruction",
                })

        elif rule_type == "drop_column":
            col = rule.get("column")
            if not col or col not in cols:
                continue
            df = df.drop(columns=[col])
            cols = set(df.columns)
            fix_log.append({
                "fix_type": "drop_column",
                "row": "N/A",
                "column": col,
                "original": col,
                "fixed": "(removed)",
                "action": f"Dropped column '{col}' per user instruction",
                "fixed_by": "User Instruction",
            })

    return df, fix_log
