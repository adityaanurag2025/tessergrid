# cleaner/domains.py
# Domain profiles: enum maps and business validation rules for 7 domains.
# Part of Tessergrid - AI Data Cleaner

import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scanner import col_index_to_letter
from cleaner.enums import (
    STATUS_MAP, UNIT_MAP, CURRENCY_MAP, REGION_MAP,
    TRANSPORT_MAP, WAREHOUSE_MAP, COUNTRY_CANONICAL,
)

# ── Domain-specific enum maps ─────────────────────────────────────────────

_SALES_CRM_STATUS_MAP = {
    "new": "New", "open": "New",
    "contacted": "Contacted",
    "qualified": "Qualified", "discovery": "Qualified",
    "proposal": "Proposal", "proposal sent": "Proposal",
    "negotiation": "Negotiation", "in talks": "Negotiation",
    "won": "Closed-Won", "closed won": "Closed-Won", "closed-won": "Closed-Won",
    "lost": "Closed-Lost", "closed lost": "Closed-Lost", "closed-lost": "Closed-Lost",
    "churned": "Churned",
}

_PRIORITY_MAP = {
    "high": "High", "h": "High",
    "medium": "Medium", "med": "Medium", "m": "Medium",
    "low": "Low", "l": "Low",
}

_MARKETING_CHANNEL_MAP = {
    "fb": "Facebook", "facebook": "Facebook",
    "ig": "Instagram", "instagram": "Instagram",
    "google": "Google", "google ads": "Google", "adwords": "Google",
    "linkedin": "LinkedIn", "li": "LinkedIn",
    "email": "Email",
    "organic": "Organic",
    "direct": "Direct",
    "referral": "Referral",
    "paid search": "Paid Search", "ppc": "Paid Search",
}

_FINANCE_STATUS_MAP = {
    "paid": "Paid",
    "unpaid": "Unpaid", "outstanding": "Unpaid",
    "overdue": "Overdue", "past due": "Overdue",
    "partial": "Partial", "partially paid": "Partial",
    "void": "Void", "voided": "Void",
    "draft": "Draft",
    "pending": "Pending",
}

_HR_STATUS_MAP = {
    "active": "Active",
    "terminated": "Terminated", "term": "Terminated",
    "on leave": "On Leave", "leave": "On Leave",
    "contract": "Contractor", "contractor": "Contractor",
    "ft": "Full-Time", "full time": "Full-Time", "full-time": "Full-Time",
    "pt": "Part-Time", "part time": "Part-Time", "part-time": "Part-Time",
    "intern": "Intern",
    "resigned": "Resigned",
}

# ── Domain profiles ───────────────────────────────────────────────────────

_DOMAIN_PROFILES = {
    "supply_chain": {
        "label": "Supply Chain",
        "enum_maps": {
            "status":         STATUS_MAP,
            "unit":           UNIT_MAP,
            "currency":       CURRENCY_MAP,
            "region":         REGION_MAP,
            "transport mode": TRANSPORT_MAP,
            "transport_mode": TRANSPORT_MAP,
            "warehouse":      WAREHOUSE_MAP,
        },
        "business_rules": ["no_negative_quantity", "no_negative_price"],
        "pii_patterns": [],
    },
    "sales_crm": {
        "label": "Sales & CRM",
        "enum_maps": {
            "stage":    _SALES_CRM_STATUS_MAP,
            "status":   _SALES_CRM_STATUS_MAP,
            "pipeline": _SALES_CRM_STATUS_MAP,
            "priority": _PRIORITY_MAP,
            "currency": CURRENCY_MAP,
        },
        "business_rules": ["no_negative_amount"],
        "pii_patterns": [],
    },
    "marketing": {
        "label": "Marketing",
        "enum_maps": {
            "channel":  _MARKETING_CHANNEL_MAP,
            "source":   _MARKETING_CHANNEL_MAP,
            "currency": CURRENCY_MAP,
        },
        "business_rules": ["no_negative_spend"],
        "pii_patterns": [],
    },
    "finance": {
        "label": "Finance & Accounting",
        "enum_maps": {
            "status":   _FINANCE_STATUS_MAP,
            "currency": CURRENCY_MAP,
        },
        "business_rules": [],
        "pii_patterns": ["ssn", "tax_id", "bank", "account_number"],
    },
    "hr": {
        "label": "HR & People",
        "enum_maps": {
            "status":          _HR_STATUS_MAP,
            "employment":      _HR_STATUS_MAP,
            "employment type": _HR_STATUS_MAP,
            "currency":        CURRENCY_MAP,
        },
        "business_rules": ["no_negative_salary"],
        "pii_patterns": ["ssn", "tax_id", "dob", "bank", "account_number", "salary", "compensation"],
    },
    "contacts": {
        "label": "Customer / Contacts",
        "enum_maps": {
            "country":  COUNTRY_CANONICAL,
            "currency": CURRENCY_MAP,
        },
        "business_rules": [],
        "pii_patterns": [],
    },
    "general": {
        "label": "General (auto-detect)",
        "enum_maps": {},
        "business_rules": [],
        "pii_patterns": [],
    },
}

DOMAIN_PROFILES = _DOMAIN_PROFILES  # public alias for __init__.py re-export

_NULL_LIKE = {"", "nan", "none", "null", "na", "n/a", "-", "--"}


def get_domain_profile(domain_key: str) -> dict:
    """Return the profile dict for a domain key. Falls back to 'general'."""
    return _DOMAIN_PROFILES.get(domain_key, _DOMAIN_PROFILES["general"])


def apply_domain_enums(df, domain_key: str):
    """
    Apply the enum maps for a given domain to the DataFrame.
    Returns: (df, fix_log)
    """
    profile = get_domain_profile(domain_key)
    enum_maps = profile["enum_maps"]
    fix_log = []
    cols = list(df.columns)

    for col_idx, col in enumerate(cols):
        col_key = col.strip().lower()
        mapping = enum_maps.get(col_key)
        if mapping is None:
            continue
        for idx, val in enumerate(df[col]):
            val_str = str(val).strip()
            if val_str.lower() in _NULL_LIKE:
                continue
            key = re.sub(r"\s+", " ", val_str.lower())
            if key in mapping:
                canonical = mapping[key]
                if canonical and canonical != val_str:
                    fix_log.append({
                        "fix_type": "inconsistent_text",
                        "row": idx + 2,
                        "column": col,
                        "original": val_str,
                        "fixed": canonical,
                        "action": f"Standardized '{val_str}' → '{canonical}'",
                        "fixed_by": "Rule-Based",
                    })
                    df.at[idx, col] = canonical
    return df, fix_log


def apply_domain_business_rules(df, domain_key: str):
    """
    Apply domain-specific business rule validation flags.
    Does NOT modify data — returns (df, flags).
    """
    profile = get_domain_profile(domain_key)
    rules = profile["business_rules"]
    flags = []
    cols_lower = {c.lower(): c for c in df.columns}

    def _flag_negative(col_pattern: str, label: str):
        matched = next((c for k, c in cols_lower.items() if col_pattern in k), None)
        if matched is None:
            return
        for idx, val in enumerate(df[matched]):
            try:
                n = float(val)
            except (TypeError, ValueError):
                continue
            if n < 0:
                flags.append({
                    "row":        idx + 2,
                    "column":     matched,
                    "col_letter": col_index_to_letter(list(df.columns).index(matched)),
                    "note":       f"Negative {label} value ({n}) — review required",
                    "flag_type":  "business_rule",
                    "issue":      f"Negative {label}",
                    "original":   str(val),
                })

    if "no_negative_quantity" in rules:
        _flag_negative("quantity", "Quantity")
        _flag_negative("qty", "Quantity")
    if "no_negative_price" in rules:
        _flag_negative("price", "Price")
    if "no_negative_amount" in rules:
        _flag_negative("amount", "Amount")
        _flag_negative("value", "Value")
    if "no_negative_spend" in rules:
        _flag_negative("spend", "Spend")
        _flag_negative("cost", "Cost")
        _flag_negative("budget", "Budget")
    if "no_negative_salary" in rules:
        _flag_negative("salary", "Salary")

    return df, flags
