# main.py
# Streamlit frontend for Tessergrid.
# Users upload messy Excel/CSV files, see scan results,
# and download clean files and fix reports from here.
# Part of Tessergrid - AI Data Cleaner
# Run from project root: streamlit run app/main.py

import io
import os
import sys
import time
import threading
import traceback
from datetime import datetime
from pathlib import Path
from collections import defaultdict, deque

import pandas as pd
import streamlit as st
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# ── Path setup ────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from scanner import (
    load_dataframe, check_duplicate_columns, check_mixed_date_formats,
    check_inconsistent_text, check_missing_values, check_extra_whitespace,
    check_numbers_as_text, check_empty_rows, check_duplicate_rows,
    check_merged_cells, group_problems_by_type,
)
from cleaner import (
    normalize_nulls, unmerge_and_load, fix_duplicate_columns, fix_empty_rows, fix_erp_footer_rows,
    fix_duplicate_rows, fix_duplicate_by_business_key, fix_whitespace, fix_customer_id,
    fix_numbers_as_text, fix_dates,
    fix_enum_cols, fix_country_names, build_inconsistent_text_summary, call_claude_for_mappings,
    majority_vote_fallback, apply_text_mappings, recalculate_total_value,
    flag_missing_values, apply_data_quality_flags, save_clean_file,
)
from reporter import generate_report

# Only load .env locally — on Streamlit Cloud secrets are injected into the environment directly
if load_dotenv and (ROOT / ".env").exists():
    load_dotenv(ROOT / ".env")

DATA_INPUT  = ROOT / "data" / "input"
DATA_OUTPUT = ROOT / "data" / "output"
DATA_INPUT.mkdir(parents=True, exist_ok=True)
DATA_OUTPUT.mkdir(parents=True, exist_ok=True)

# ── Problem metadata ──────────────────────────────────────────────────────
PROBLEM_META = {
    "DUPLICATE COLUMNS":        {"label": "Duplicate Columns",     "severity": "High",   "fix_type": "duplicate_columns"},
    "MIXED DATE FORMATS":       {"label": "Mixed Date Formats",     "severity": "High",   "fix_type": "mixed_dates"},
    "NUMBERS STORED AS TEXT":   {"label": "Numbers Stored as Text", "severity": "High",   "fix_type": "numbers_fixed"},
    "INCONSISTENT TEXT VALUES": {"label": "Inconsistent Text",      "severity": "Medium", "fix_type": "inconsistent_text"},
    "DUPLICATE ROWS":           {"label": "Duplicate Rows",         "severity": "Medium", "fix_type": "duplicate_rows"},
    "MERGED CELLS":             {"label": "Merged Cells",           "severity": "Medium", "fix_type": "merged_cells"},
    "EXTRA WHITESPACE":         {"label": "Extra Whitespace",       "severity": "Low",    "fix_type": "whitespace_cleaned"},
    "EMPTY ROWS":               {"label": "Empty Rows",             "severity": "Low",    "fix_type": "empty_rows"},
    "MISSING VALUES":           {"label": "Missing Values",         "severity": "Low",    "fix_type": None},
}

REMOVED_FIX_TYPES = {"empty_rows", "erp_footer_rows", "duplicate_rows", "duplicate_columns"}

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

MAX_UPLOAD_BYTES = 12 * 1024 * 1024  # 12 MB

# Magic bytes for Excel formats — used to validate uploads beyond just extension
_XLSX_MAGIC = b'PK\x03\x04'       # xlsx is a ZIP archive
_XLS_MAGIC  = b'\xD0\xCF\x11\xE0' # legacy xls (OLE2 compound doc)

# ── Rate limiting ─────────────────────────────────────────────────────────
# Global: max 20 clean operations per minute across ALL users (shared process)
_GLOBAL_RATE_LOCK   = threading.Lock()
_GLOBAL_REQUEST_LOG = deque()          # timestamps of recent global requests
MAX_GLOBAL_RPM      = 20

# Per-session: max 5 cleans, with a 60-second cooldown between each
MAX_CLEANS_PER_SESSION = 5
CLEAN_COOLDOWN_SECONDS = 60


def _global_rate_limit_check() -> tuple[bool, int]:
    """
    Sliding-window global rate limiter.
    Returns (allowed: bool, retry_after_seconds: int).
    """
    now = time.time()
    with _GLOBAL_RATE_LOCK:
        # Evict entries older than 60 seconds
        while _GLOBAL_REQUEST_LOG and _GLOBAL_REQUEST_LOG[0] < now - 60:
            _GLOBAL_REQUEST_LOG.popleft()
        if len(_GLOBAL_REQUEST_LOG) >= MAX_GLOBAL_RPM:
            oldest = _GLOBAL_REQUEST_LOG[0]
            retry_after = int(60 - (now - oldest)) + 1
            return False, retry_after
        _GLOBAL_REQUEST_LOG.append(now)
        return True, 0


# ── CSS ───────────────────────────────────────────────────────────────────

def inject_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Design Tokens (The Pristine Ledger) ────────────────── */
:root {
    --surface:          #f7f9fd;
    --surface-low:      #f2f4f8;
    --surface-lowest:   #ffffff;
    --surface-highest:  #e8eaf0;
    --primary:          #000000;
    --primary-grad:     #131b2e;
    --on-primary:       #ffffff;
    --tertiary:         #6ffbbe;
    --on-tertiary:      #003823;
    --on-surface:       #191c1f;
    --on-surface-var:   #44474f;
    --outline-ghost:    rgba(198,198,205,0.18);
    --shadow-ambient:   rgba(25,28,31,0.05);
    --shadow-float:     rgba(25,28,31,0.08);
    --error:            #ba1a1a;
    --warning-bg:       #fffbf0;
    --warning-border:   #e6a817;
    --info-bg:          #f0f6ff;
    --info-border:      #2563eb;
    --font:             'Inter', system-ui, sans-serif;
}

/* ── Keyframes ──────────────────────────────────────────── */
@keyframes fadeUp {
    from { opacity:0; transform:translateY(14px); }
    to   { opacity:1; transform:translateY(0); }
}

/* ── App Background ─────────────────────────────────────── */
.stApp {
    background-color: var(--surface) !important;
    font-family: var(--font) !important;
}

.main .block-container {
    padding-top: 2rem !important;
    max-width: 940px;
}

/* ── Hide Streamlit chrome ──────────────────────────────── */
#MainMenu, footer, .stDeployButton { visibility: hidden !important; }
header[data-testid="stHeader"] {
    background: rgba(247,249,253,0.85) !important;
    backdrop-filter: blur(16px);
}

/* ── Sidebar ─────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: var(--surface-low) !important;
}
[data-testid="stSidebar"] * {
    font-family: var(--font) !important;
}
[data-testid="stSidebar"] hr {
    border-color: var(--outline-ghost) !important;
    margin: 14px 0 !important;
}
[data-testid="stSidebar"] .stCaption {
    color: var(--on-surface-var) !important;
    font-size: 0.72rem !important;
}

/* ── Secondary / ghost buttons ───────────────────────────── */
.stButton > button {
    background: var(--surface-lowest) !important;
    color: var(--on-surface) !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 12px 24px !important;
    font-family: var(--font) !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    transition: all 0.18s ease !important;
    width: 100% !important;
    box-shadow: 0 1px 4px var(--shadow-ambient) !important;
}
.stButton > button:hover {
    background: var(--surface-highest) !important;
    box-shadow: 0 4px 20px var(--shadow-float) !important;
    transform: translateY(-1px) !important;
}

/* ── Primary CTA buttons (gradient) ─────────────────────── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, var(--primary) 0%, var(--primary-grad) 100%) !important;
    color: var(--on-primary) !important;
    border: none !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.18) !important;
    letter-spacing: 0.06em !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 6px 28px rgba(0,0,0,0.28) !important;
    transform: translateY(-2px) !important;
}

/* ── Clean Data / tertiary action button ─────────────────── */
button[kind="primary"][data-cf-tertiary="true"],
.cf-btn-tertiary > .stButton > button[kind="primary"] {
    background: var(--tertiary) !important;
    color: var(--on-tertiary) !important;
    box-shadow: 0 2px 16px rgba(111,251,190,0.35) !important;
}

/* ── Download buttons ────────────────────────────────────── */
.stDownloadButton > button {
    background: var(--surface-lowest) !important;
    color: var(--on-surface) !important;
    border: none !important;
    border-radius: 12px !important;
    font-family: var(--font) !important;
    font-size: 0.80rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    transition: all 0.18s ease !important;
    width: 100% !important;
    box-shadow: 0 1px 4px var(--shadow-ambient) !important;
}
.stDownloadButton > button[kind="primary"] {
    background: linear-gradient(135deg, var(--primary) 0%, var(--primary-grad) 100%) !important;
    color: var(--on-primary) !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.18) !important;
}
.stDownloadButton > button:hover {
    box-shadow: 0 4px 20px var(--shadow-float) !important;
    transform: translateY(-1px) !important;
}

/* ── File Uploader — Glass Dropzone ──────────────────────── */
[data-testid="stFileUploader"] {
    background: transparent !important;
    border: none !important;
}
[data-testid="stFileUploaderDropzoneInstructions"],
[data-testid="stFileUploaderDropzone"] {
    background: rgba(255,255,255,0.72) !important;
    backdrop-filter: blur(16px) !important;
    -webkit-backdrop-filter: blur(16px) !important;
    border-radius: 16px !important;
    border: none !important;
    box-shadow: 0 2px 16px var(--shadow-ambient) !important;
    transition: all 0.24s ease !important;
}
[data-testid="stFileUploaderDropzone"]:hover {
    background: rgba(255,255,255,0.92) !important;
    box-shadow: 0 6px 32px var(--shadow-float) !important;
}

/* ── Progress Bar ────────────────────────────────────────── */
[data-testid="stProgress"] > div > div > div > div {
    background: linear-gradient(90deg, var(--primary), var(--primary-grad)) !important;
    border-radius: 99px !important;
}
[data-testid="stProgress"] > div > div {
    background: var(--surface-highest) !important;
    border-radius: 99px !important;
}
[data-testid="stProgress"] p {
    font-family: var(--font) !important;
    font-size: 0.75rem !important;
    color: var(--on-surface-var) !important;
    letter-spacing: 0.05em !important;
}

/* ── Dataframe ───────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    background: var(--surface-lowest) !important;
    border-radius: 12px !important;
    overflow: hidden;
    box-shadow: 0 2px 12px var(--shadow-ambient) !important;
}

/* ── Alert / notice boxes ────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 10px !important;
    font-family: var(--font) !important;
    font-size: 0.83rem !important;
    border: none !important;
}
[data-testid="stInfo"] {
    background: var(--info-bg) !important;
    border-left: 3px solid var(--info-border) !important;
}
[data-testid="stSuccess"] {
    background: #f0fdf6 !important;
    border-left: 3px solid #16a34a !important;
}
[data-testid="stWarning"] {
    background: var(--warning-bg) !important;
    border-left: 3px solid var(--warning-border) !important;
}
[data-testid="stError"] {
    background: #fff0f0 !important;
    border-left: 3px solid var(--error) !important;
}

/* ── Spinner ─────────────────────────────────────────────── */
[data-testid="stSpinner"] > div {
    border-top-color: var(--primary) !important;
}

/* ── Caption / small text ────────────────────────────────── */
.stCaption { color: var(--on-surface-var) !important; }


/* ── Custom layout components ────────────────────────────── */
.cf-logo {
    font-family: var(--font);
    font-size: 3.2rem;
    font-weight: 800;
    color: var(--on-surface);
    text-align: center;
    letter-spacing: -0.02em;
    animation: fadeUp 0.5s ease both;
    line-height: 1;
    margin-bottom: 0;
}
.cf-logo em {
    color: var(--primary);
    font-style: normal;
    background: linear-gradient(135deg, #000000, #131b2e);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.cf-tagline {
    font-family: var(--font);
    font-size: 0.72rem;
    color: var(--on-surface-var);
    text-align: center;
    letter-spacing: 0.20em;
    text-transform: uppercase;
    font-weight: 500;
    animation: fadeUp 0.5s 0.14s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
    margin-top: 8px;
}

.cf-upload-box {
    position: relative;
    background: var(--surface-lowest);
    border-radius: 20px;
    padding: 36px 32px 28px;
    margin-top: 24px;
    animation: fadeUp 0.5s 0.26s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
    box-shadow: 0 4px 32px var(--shadow-ambient);
}
.cf-upload-hint {
    font-family: var(--font);
    font-size: 0.73rem;
    color: var(--on-surface-var);
    text-align: center;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    font-weight: 500;
    margin-bottom: 16px;
    display: block;
}

.cf-metric-card {
    background: var(--surface-lowest);
    border-radius: 16px;
    padding: 24px 16px;
    text-align: center;
    animation: fadeUp 0.4s ease both;
    transition: box-shadow 0.2s, transform 0.2s;
    box-shadow: 0 2px 12px var(--shadow-ambient);
}
.cf-metric-card:hover {
    box-shadow: 0 6px 28px var(--shadow-float);
    transform: translateY(-2px);
}
.cf-metric-icon  { font-size: 1.3rem; display: block; margin-bottom: 10px; }
.cf-metric-label {
    font-family: var(--font);
    font-size: 0.63rem;
    color: var(--on-surface-var);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    font-weight: 600;
    display: block;
    margin-bottom: 8px;
}
.cf-metric-value {
    font-family: var(--font);
    font-size: 1.6rem;
    font-weight: 800;
    color: var(--on-surface);
    display: block;
    line-height: 1.15;
    word-break: break-all;
    letter-spacing: -0.02em;
}
.cf-metric-value.sm {
    font-size: 0.83rem;
    font-weight: 500;
    color: var(--on-surface-var);
    letter-spacing: 0;
}

.cf-rule {
    display: flex;
    align-items: center;
    gap: 12px;
    font-family: var(--font);
    font-size: 0.68rem;
    color: var(--on-surface-var);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    font-weight: 600;
    margin: 28px 0 14px;
}
.cf-rule::before { content:''; flex:none; width:16px; height:2px; background:var(--on-surface); border-radius:2px; }
.cf-rule::after  { content:''; flex:1; height:1px; background:var(--outline-ghost); }

.cf-success-banner {
    background: var(--surface-lowest);
    border-radius: 20px;
    padding: 40px 28px;
    text-align: center;
    box-shadow: 0 4px 32px var(--shadow-ambient);
    animation: fadeUp 0.5s ease both;
}
.cf-success-icon  { font-size: 2.4rem; display: block; margin-bottom: 14px; }
.cf-success-title {
    font-family: var(--font);
    font-size: 1.8rem;
    font-weight: 800;
    color: var(--on-surface);
    margin: 0 0 10px;
    letter-spacing: -0.02em;
}
.cf-success-sub {
    font-family: var(--font);
    font-size: 0.83rem;
    color: var(--on-surface-var);
    font-weight: 400;
    max-width: 480px;
    margin: 0 auto;
    line-height: 1.6;
}

.cf-sidebar-brand {
    font-family: var(--font);
    font-size: 1.05rem;
    font-weight: 800;
    color: var(--on-surface);
    letter-spacing: -0.01em;
    display: block;
    margin-bottom: 4px;
}
.cf-sidebar-desc {
    font-family: var(--font);
    font-size: 0.74rem;
    color: var(--on-surface-var);
    line-height: 1.65;
}
.cf-issue-row {
    display: flex;
    align-items: center;
    gap: 10px;
    font-family: var(--font);
    font-size: 0.76rem;
    color: var(--on-surface-var);
    margin: 6px 0;
    font-weight: 400;
}
.cf-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    flex: none;
}
.cf-dot.high   { background: #dc2626; }
.cf-dot.medium { background: #d97706; }
.cf-dot.low    { background: #16a34a; }

.cf-scan-title {
    font-family: var(--font);
    font-size: 2.0rem;
    font-weight: 800;
    color: var(--on-surface);
    margin-bottom: 4px;
    animation: fadeUp 0.4s ease both;
    letter-spacing: -0.02em;
}
.cf-scan-sub {
    font-family: var(--font);
    font-size: 0.82rem;
    color: var(--on-surface-var);
    animation: fadeUp 0.4s 0.10s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
}
.cf-scan-sub b { color: var(--on-surface); font-weight: 600; }

.cf-sev-badge {
    display: inline-block;
    font-family: var(--font);
    font-size: 0.64rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 3px 8px;
    border-radius: 99px;
    font-weight: 600;
}
.cf-sev-badge.high   { color: #dc2626; background: rgba(220,38,38,0.08); }
.cf-sev-badge.medium { color: #d97706; background: rgba(217,119,6,0.08); }
.cf-sev-badge.low    { color: #16a34a; background: rgba(22,163,74,0.08); }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────

def show_sidebar():
    with st.sidebar:
        st.markdown(
            "<span class='cf-sidebar-brand'>⬡ TESSERGRID</span>"
            "<span class='cf-sidebar-desc'>AI-powered cleaner for messy spreadsheets. "
            "Pick your data domain and we'll apply the right rules.</span>",
            unsafe_allow_html=True,
        )
        st.divider()
        st.markdown("<div style='font-family:Inter,sans-serif;font-size:0.68rem;color:#44474f;letter-spacing:0.14em;text-transform:uppercase;font-weight:600;margin-bottom:10px;'>Supported Issues</div>", unsafe_allow_html=True)

        issues = [
            ("high",   "Duplicate column names"),
            ("high",   "Mixed date formats"),
            ("high",   "Numbers stored as text"),
            ("medium", "Inconsistent text values"),
            ("medium", "Duplicate rows"),
            ("medium", "Merged cells (ERP exports)"),
            ("low",    "Extra whitespace"),
            ("low",    "Completely empty rows"),
            ("low",    "Missing values (flagged)"),
        ]
        rows_html = "".join(
            f"<div class='cf-issue-row'><span class='cf-dot {sev}'></span>{label}</div>"
            for sev, label in issues
        )
        st.markdown(rows_html, unsafe_allow_html=True)
        st.divider()
        st.caption("Version: v1.0.0")
        st.markdown(
            "<div style='font-family:Inter,sans-serif;font-size:0.72rem;color:#44474f;margin-top:2px;'>"
            "Built by <strong style='color:#191c1f;'>Aditya Anurag</strong>"
            "</div>",
            unsafe_allow_html=True,
        )
        st.divider()
        st.warning(
            "Beta Notice: Tessergrid is an early-stage product. Please review cleaned files and fix reports "
            "before using them in production or business-critical workflows."
        )

        if not os.getenv("ANTHROPIC_API_KEY"):
            st.divider()
            st.warning(
                "ANTHROPIC_API_KEY is not set. "
                "Text standardization will use rule-based fallback instead of Claude AI.",
                icon="⚠️",
            )

        st.divider()
        st.markdown("""
<details style="margin-bottom:8px;">
<summary style="font-family:Inter,sans-serif;font-size:0.80rem;font-weight:600;color:#191c1f;cursor:pointer;padding:10px 14px;background:#ffffff;border-radius:10px;list-style:revert;">Privacy Policy</summary>
<div style="font-family:Inter,sans-serif;font-size:0.74rem;color:#44474f;line-height:1.7;padding:14px 14px 10px;background:#ffffff;border-radius:0 0 10px 10px;">
<b>Tessergrid — Privacy Policy</b><br><br>
<b>1. Information We Process</b><br>Tessergrid processes files uploaded by users for the purpose of data cleaning, issue detection, and report generation.<br><br>
<b>2. Uploaded File Content</b><br>Uploaded files may include business data such as order records, SKU data, supplier information, warehouse details, and other tabular operational data.<br><br>
<b>3. Purpose of Processing</b><br>Uploaded files are processed only to detect data quality issues, clean supported issues automatically, generate a fix report, and return output files to the user.<br><br>
<b>4. Third-Party Processing</b><br>Tessergrid uses the Anthropic Claude API to assist with cleaning operations. By using Tessergrid, you acknowledge that uploaded data may be transmitted to Anthropic strictly for the purpose of providing cleaning and reporting functionality.<br><br>
<b>5. Data Retention</b><br>Uploaded files are processed in-session and are not retained by Tessergrid after the session ends.<br><br>
<b>6. Model Training</b><br>Uploaded files are not used by Tessergrid to train any AI model.<br><br>
<b>7. User Responsibility</b><br>Users are responsible for ensuring they have the right to upload and process the files they submit.<br><br>
<b>8. Security</b><br>Tessergrid applies reasonable safeguards during file processing. Users should not upload confidential or regulated datasets without organizational approval.<br><br>
<b>9. Contact</b><br>For privacy-related questions, contact: <a href="mailto:adityaanurag2024@gmail.com" style="color:#191c1f;">adityaanurag2024@gmail.com</a>
</div>
</details>

<details>
<summary style="font-family:Inter,sans-serif;font-size:0.80rem;font-weight:600;color:#191c1f;cursor:pointer;padding:10px 14px;background:#ffffff;border-radius:10px;list-style:revert;">Terms of Use</summary>
<div style="font-family:Inter,sans-serif;font-size:0.74rem;color:#44474f;line-height:1.7;padding:14px 14px 10px;background:#ffffff;border-radius:0 0 10px 10px;">
<b>Tessergrid — Terms of Use</b><br><br>
<b>1. Service Description</b><br>Tessergrid is an AI-powered data cleaning tool for Excel and CSV supply chain datasets.<br><br>
<b>2. Beta Product Notice</b><br>Tessergrid is an early-stage product and may not detect or resolve every issue in every dataset.<br><br>
<b>3. User Review Required</b><br>Users must review cleaned outputs and reports before using them in operational, financial, or business-critical workflows.<br><br>
<b>4. No Warranty</b><br>The service is provided as-is, without guarantees of completeness, accuracy, or fitness for any specific business purpose.<br><br>
<b>5. Limitation of Liability</b><br>Tessergrid and its creator are not responsible for downstream business decisions made using cleaned outputs.<br><br>
<b>6. Acceptable Use</b><br>Users must not upload unlawful content, malicious files, or data they are not authorized to process.<br><br>
<b>7. Ownership of Data</b><br>Users retain ownership of their uploaded files and output files.<br><br>
<b>8. Service Availability</b><br>The service may be modified, suspended, or discontinued at any time.<br><br>
<b>9. Contact</b><br>For support, contact: <a href="mailto:adityaanurag2024@gmail.com" style="color:#191c1f;">adityaanurag2024@gmail.com</a>
</div>
</details>
""", unsafe_allow_html=True)

        st.markdown(
            "<div style='font-family:Inter,sans-serif;font-size:0.72rem;color:#44474f;margin-top:4px;'>"
            "Contact<br><a href='mailto:adityaanurag2024@gmail.com' style='color:#191c1f;font-weight:600;text-decoration:none;'>"
            "adityaanurag2024@gmail.com</a>"
            "</div>",
            unsafe_allow_html=True,
        )


# ── File helpers ──────────────────────────────────────────────────────────

def _validate_file_magic(uploaded_file, ext: str) -> None:
    """Checks magic bytes to verify Excel files match their declared extension."""
    header = uploaded_file.read(4)
    uploaded_file.seek(0)
    if ext == ".xlsx" and not header.startswith(_XLSX_MAGIC):
        raise ValueError("File does not appear to be a valid .xlsx file (magic bytes mismatch).")
    if ext == ".xls" and not header.startswith(_XLS_MAGIC):
        raise ValueError("File does not appear to be a valid .xls file (magic bytes mismatch).")


def save_uploaded_file(uploaded_file):
    # Strip directory components to prevent path traversal attacks
    name = Path(uploaded_file.name).name
    ext  = Path(name).suffix.lower()

    # Validate Excel files via magic bytes before writing to disk
    if ext in (".xlsx", ".xls"):
        _validate_file_magic(uploaded_file, ext)

    if ext == ".csv":
        raw = uploaded_file.read()
        df = None
        for encoding in ("utf-8-sig", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding=encoding)
                break
            except (UnicodeDecodeError, Exception):
                continue
        if df is None:
            raise ValueError("Could not decode CSV file — tried UTF-8, latin-1, and cp1252.")
        df.columns = [c.lstrip("\ufeff") for c in df.columns]
        out_path = DATA_INPUT / (Path(name).stem + ".xlsx")
        df.to_excel(out_path, index=False)
    else:
        out_path = DATA_INPUT / name
        out_path.write_bytes(uploaded_file.read())
    return str(out_path)


@st.cache_data
def cached_load_dataframe(filepath):
    return load_dataframe(filepath)


def run_scan(filepath):
    df = cached_load_dataframe(filepath)
    all_problems = []
    all_problems += check_duplicate_columns(df)
    all_problems += check_mixed_date_formats(df)
    all_problems += check_inconsistent_text(df)
    all_problems += check_missing_values(df)
    all_problems += check_extra_whitespace(df)
    all_problems += check_numbers_as_text(filepath)
    all_problems += check_empty_rows(df)
    all_problems += check_duplicate_rows(df)
    all_problems += check_merged_cells(filepath)
    grouped = group_problems_by_type(all_problems)
    return {
        "filepath":       filepath,
        "total_rows":     len(df),
        "total_cols":     len(df.columns),
        "grouped":        dict(grouped),
        "total_problems": len(all_problems),
    }


def run_clean_with_progress(filepath):
    bar    = st.progress(0, text="Initializing…")
    status = st.empty()

    def step(pct, msg):
        bar.progress(pct, text=msg)
        status.caption(f"↳ {msg}")

    step(5,  "Unmerging merged cells…")
    df, unmerged_count = unmerge_and_load(filepath)
    rows_before = len(df)

    step(10, "Normalizing null placeholders…")
    df, log_nulls = normalize_nulls(df)

    step(18, "Removing duplicate columns…")
    df, log_dup_cols = fix_duplicate_columns(df)

    step(25, "Removing empty and ERP footer rows…")
    df, log_empty  = fix_empty_rows(df)
    df, log_footer = fix_erp_footer_rows(df)

    step(38, "Cleaning whitespace…")
    df, log_ws = fix_whitespace(df)

    step(42, "Normalizing customer IDs…")
    df, log_cust = fix_customer_id(df)

    step(48, "Converting numbers stored as text…")
    df, log_nums = fix_numbers_as_text(df)

    step(57, "Standardizing date formats…")
    df, log_dates = fix_dates(df)

    step(65, "Standardizing enum columns…")
    df, log_enums, enum_flags = fix_enum_cols(df)

    step(70, "Standardizing country names…")
    df, log_countries = fix_country_names(df)

    step(76, "Calling Claude AI for text standardization…")
    text_summary = build_inconsistent_text_summary(df)
    api_response  = call_claude_for_mappings(text_summary)

    if api_response and "mappings" in api_response:
        mappings = api_response["mappings"]
        fixed_by = "Claude AI"
    else:
        mappings = majority_vote_fallback(df, text_summary)
        fixed_by = "Rule-Based"

    df, log_text = apply_text_mappings(df, mappings, fixed_by=fixed_by)

    step(84, "Removing duplicate rows by business key…")
    df, log_dupes     = fix_duplicate_rows(df)
    df, log_biz_dupes = fix_duplicate_by_business_key(df)

    step(90, "Recalculating Total Value column…")
    df, log_totals = recalculate_total_value(df)

    step(95, "Applying business rule validation…")
    df, quality_flags = apply_data_quality_flags(df)
    flags = flag_missing_values(df) + quality_flags + enum_flags

    step(98, "Saving clean file…")
    original_filename = st.session_state.get("original_filename", "output")
    stem = Path(original_filename).stem
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_filename  = f"clean_{stem}_{ts}.xlsx"
    report_filename = f"tessergrid_report_{stem}_{ts}.xlsx"
    output_path     = str(DATA_OUTPUT / clean_filename)
    report_path_str = str(DATA_OUTPUT / report_filename)
    st.session_state["clean_path"]  = output_path
    st.session_state["report_path"] = report_path_str
    save_clean_file(df, output_path)

    all_fixes = (
        log_nulls + log_dup_cols + log_empty + log_footer +
        log_ws + log_cust + log_nums + log_dates + log_enums + log_countries + log_text +
        log_dupes + log_biz_dupes + log_totals
    )

    report_data = {
        "input_file":     original_filename or filepath,
        "output_file":    output_path,
        "rows_before":    rows_before,
        "rows_after":     len(df),
        "fix_log":        all_fixes,
        "flags":          flags,
        "unmerged_count": unmerged_count,
    }

    step(99, "Generating Excel report…")
    generate_report(report_data, report_path_str)

    bar.progress(100, text="Complete")
    status.empty()

    return report_data


# ── Pages ─────────────────────────────────────────────────────────────────

def show_upload_page():
    st.markdown("<br>", unsafe_allow_html=True)

    # Logo
    st.markdown(
        "<div class='cf-logo'>Tesser<em>grid</em></div>"
        "<div class='cf-tagline'>AI-Powered Data Cleaner for Any Spreadsheet</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    st.info(
        "Data Handling Notice: Uploaded files are processed only for cleaning and report generation. "
        "Please avoid uploading highly sensitive or regulated data unless approved by your organization. "
        "Review your company's data-sharing policy before use."
    )

    # Upload box
    st.markdown("<div class='cf-upload-box'>", unsafe_allow_html=True)
    st.markdown(
        "<span class='cf-upload-hint'>Drop your Excel or CSV file below</span>",
        unsafe_allow_html=True,
    )
    uploaded = st.file_uploader(
        "Choose file",
        type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if uploaded is not None:
        if uploaded.size == 0:
            st.error("The uploaded file is empty. Please upload a file with data.")
            return

        if uploaded.size > MAX_UPLOAD_BYTES:
            st.error(
                f"File is too large ({uploaded.size / (1024 * 1024):.1f} MB). "
                f"Maximum allowed size is 12 MB."
            )
            return

        with st.spinner("Reading file…"):
            try:
                filepath   = save_uploaded_file(uploaded)
                df_preview = cached_load_dataframe(filepath)
            except Exception:
                print(traceback.format_exc())
                st.error("Could not read the uploaded file. Please check the format and try again.")
                return

        st.markdown("<br>", unsafe_allow_html=True)

        # Three metric cards
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(
                f"<div class='cf-metric-card' style='animation-delay:0.05s'>"
                f"<span class='cf-metric-icon'>📄</span>"
                f"<span class='cf-metric-label'>File Name</span>"
                f"<span class='cf-metric-value sm'>{uploaded.name}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                f"<div class='cf-metric-card' style='animation-delay:0.12s'>"
                f"<span class='cf-metric-icon'>⬡</span>"
                f"<span class='cf-metric-label'>Rows</span>"
                f"<span class='cf-metric-value'>{len(df_preview):,}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        with c3:
            st.markdown(
                f"<div class='cf-metric-card' style='animation-delay:0.20s'>"
                f"<span class='cf-metric-icon'>⬛</span>"
                f"<span class='cf-metric-label'>Columns</span>"
                f"<span class='cf-metric-value'>{len(df_preview.columns)}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        if st.button("Scan My Data →", type="primary", use_container_width=True):
            with st.spinner("Scanning for problems…"):
                try:
                    scan_data = run_scan(filepath)
                except Exception:
                    print(traceback.format_exc())
                    st.error("Something went wrong during scanning. Please try uploading your file again.")
                    return
            st.session_state.filepath          = filepath
            st.session_state.original_filename = uploaded.name
            st.session_state.scan_data         = scan_data
            st.session_state.stage             = "scan"
            st.rerun()


def show_scan_page():
    scan     = st.session_state.scan_data
    filename = st.session_state.original_filename
    grouped  = scan["grouped"]

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(
        "<div class='cf-scan-title'>Scan Results</div>"
        f"<div class='cf-scan-sub'>Found <b>{scan['total_problems']}</b> problems in "
        f"<b>{filename}</b> &nbsp;·&nbsp; "
        f"<b>{scan['total_rows']:,}</b> rows &nbsp;·&nbsp; "
        f"<b>{scan['total_cols']}</b> columns</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    if not grouped:
        st.success("No problems found — your data looks clean!")
    else:
        st.markdown("<div class='cf-rule'>Problems Detected</div>", unsafe_allow_html=True)

        rows = []
        for scan_type, meta in PROBLEM_META.items():
            if scan_type not in grouped:
                continue
            rows.append({
                "Problem Type": meta["label"],
                "Count":        len(grouped[scan_type]),
                "Severity":     meta["severity"],
            })
        df_problems = pd.DataFrame(rows)

        st.dataframe(
            df_problems,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Problem Type": st.column_config.TextColumn("Problem Type", width="large"),
                "Count":        st.column_config.NumberColumn("Count", width="small"),
                "Severity":     st.column_config.TextColumn("Severity", width="medium"),
            },
        )

        st.markdown(
            f"<div style='font-family:var(--font-mono);font-size:0.78rem;color:var(--text-dim);margin-top:8px;'>"
            f"Total problems: <span style='color:var(--cyan);font-weight:600;'>{scan['total_problems']}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        st.info(
            "Tessergrid will automatically fix all High and Medium severity issues. "
            "Low severity items will be flagged for your review."
        )

    st.markdown("<br>", unsafe_allow_html=True)
    col_clean, col_reset = st.columns([2, 1])

    with col_clean:
        cleans_done     = st.session_state.get("cleans_done", 0)
        last_clean_time = st.session_state.get("last_clean_time", 0.0)
        cooldown_left   = max(0, CLEAN_COOLDOWN_SECONDS - int(time.time() - last_clean_time))

        if cleans_done >= MAX_CLEANS_PER_SESSION:
            st.error(f"Session limit reached ({MAX_CLEANS_PER_SESSION} cleans). Start a new session to continue.")
        elif cooldown_left > 0:
            st.warning(f"Please wait {cooldown_left}s before running another clean.")
            st.button("⬡ Clean My Data", type="primary", use_container_width=True, disabled=True)
        elif st.button("⬡ Clean My Data", type="primary", use_container_width=True):
            # Global rate limit check
            allowed, retry_after = _global_rate_limit_check()
            if not allowed:
                st.error(f"Too many requests right now. Please try again in {retry_after}s.")
                return

            st.markdown("<div class='cf-rule'>Cleaning in Progress</div>", unsafe_allow_html=True)
            try:
                clean_data = run_clean_with_progress(st.session_state.filepath)
            except Exception:
                print(traceback.format_exc())
                st.error("Something went wrong during cleaning. Please try again or upload a different file.")
                return
            st.session_state.cleans_done    = cleans_done + 1
            st.session_state.last_clean_time = time.time()
            st.session_state.clean_data     = clean_data
            st.session_state.stage          = "clean"
            st.rerun()

    with col_reset:
        if st.button("↩ Upload Different File", use_container_width=True):
            for key in ("stage", "filepath", "original_filename", "scan_data", "clean_data"):
                st.session_state.pop(key, None)
            st.rerun()


def show_clean_page():
    clean        = st.session_state.clean_data
    scan         = st.session_state.scan_data
    rows_removed = clean["rows_before"] - clean["rows_after"]
    total_fixes  = len(clean["fix_log"]) + clean["unmerged_count"]

    # Success banner
    st.markdown(
        "<div class='cf-success-banner'>"
        "<span class='cf-success-icon'>✦</span>"
        "<div class='cf-success-title'>Data Cleaned Successfully</div>"
        "<div class='cf-success-sub'>Cleaning completed. Supported issues were fixed automatically, and unresolved items were flagged for manual review.</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # Four metric cards
    m1, m2, m3, m4 = st.columns(4)
    cards = [
        (m1, "⬡", "Rows Before",   f"{clean['rows_before']:,}", "0.04s"),
        (m2, "⬡", "Rows After",    f"{clean['rows_after']:,}",  "0.10s"),
        (m3, "↓", "Rows Removed",  f"{rows_removed:,}",         "0.16s"),
        (m4, "✦", "Fixes Applied", f"{total_fixes:,}",          "0.22s"),
    ]
    for col, icon, label, value, delay in cards:
        with col:
            st.markdown(
                f"<div class='cf-metric-card' style='animation-delay:{delay}'>"
                f"<span class='cf-metric-icon'>{icon}</span>"
                f"<span class='cf-metric-label'>{label}</span>"
                f"<span class='cf-metric-value'>{value}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # Build fix summary
    fix_counts = defaultdict(int)
    for entry in clean["fix_log"]:
        fix_counts[entry["fix_type"]] += 1
    fix_counts["merged_cells"] = clean["unmerged_count"]

    summary_rows = []
    for scan_type, meta in PROBLEM_META.items():
        found = len(scan.get("grouped", {}).get(scan_type, []))
        ft    = meta.get("fix_type")
        fixed = fix_counts.get(ft, 0)
        if found == 0 and fixed == 0:
            continue
        if ft is None:
            status = "Flagged"
        elif ft in REMOVED_FIX_TYPES:
            status = "Removed"
        else:
            status = "Fixed"
        summary_rows.append({
            "Problem Type": meta["label"],
            "Found":        found,
            "Fixed":        fixed,
            "Status":       status,
        })

    if clean["unmerged_count"] > 0:
        summary_rows.append({
            "Problem Type": "Merged Cells",
            "Found":        clean["unmerged_count"],
            "Fixed":        clean["unmerged_count"],
            "Status":       "Fixed",
        })

    df_summary = pd.DataFrame(summary_rows)

    clean_path  = Path(st.session_state.get("clean_path",  str(DATA_OUTPUT / "clean_supply_chain_data.xlsx")))
    report_path = Path(st.session_state.get("report_path", str(DATA_OUTPUT / "tessergrid_report.xlsx")))

    # Data preview
    st.markdown("<div class='cf-rule'>Data Preview — first 10 rows</div>", unsafe_allow_html=True)
    if clean_path.exists():
        try:
            df_preview = pd.read_excel(clean_path, nrows=10)
            st.dataframe(df_preview, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Could not load preview: {e}")

    st.markdown("<br>", unsafe_allow_html=True)

    # Fix summary table
    st.markdown("<div class='cf-rule'>Fix Summary</div>", unsafe_allow_html=True)
    st.dataframe(
        df_summary,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Problem Type": st.column_config.TextColumn("Problem Type", width="large"),
            "Found":        st.column_config.NumberColumn("Found",  width="small"),
            "Fixed":        st.column_config.NumberColumn("Fixed",  width="small"),
            "Status":       st.column_config.TextColumn("Status",  width="medium"),
        },
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # Downloads
    st.markdown("<div class='cf-rule'>Downloads</div>", unsafe_allow_html=True)

    if clean.get("flags"):
        st.warning(
            "Manual Review Recommended: Some records were flagged instead of auto-corrected. "
            "Please review the fix report before using the cleaned file in downstream workflows."
        )

    col_a, col_b = st.columns(2)
    with col_a:
        if clean_path.exists():
            st.download_button(
                label="↓ Download Clean File",
                data=clean_path.read_bytes(),
                file_name="clean_supply_chain_data.xlsx",
                mime=XLSX_MIME,
                use_container_width=True,
                type="primary",
            )
        else:
            st.warning("Clean file not found.")

    with col_b:
        if report_path.exists():
            st.download_button(
                label="↓ Download Fix Report",
                data=report_path.read_bytes(),
                file_name="tessergrid_report.xlsx",
                mime=XLSX_MIME,
                use_container_width=True,
            )
        else:
            st.warning("Report file not found.")

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↩ Clean Another File"):
        for key in ("stage", "filepath", "original_filename", "scan_data", "clean_data"):
            st.session_state.pop(key, None)
        st.rerun()


# ── App entry point ───────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Tessergrid",
        page_icon="⬡",
        layout="centered",
        initial_sidebar_state="expanded",
    )
    inject_css()
    show_sidebar()

    if "stage" not in st.session_state:
        st.session_state.stage = "upload"

    stage = st.session_state.stage
    if stage == "upload":
        show_upload_page()
    elif stage == "scan":
        show_scan_page()
    elif stage == "clean":
        show_clean_page()


if __name__ == "__main__":
    main()
