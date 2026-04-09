# main.py
# Streamlit frontend for ChainFix.
# Users upload messy Excel/CSV files, see scan results,
# and download clean files and fix reports from here.
# Part of ChainFix - Supply Chain Data Cleaning Tool
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
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:ital,wght@0,300;0,400;0,500;0,600;1,400&display=swap');

/* ── Variables ──────────────────────────────────────────── */
:root {
    --bg-0:         #060b17;
    --bg-1:         #0c1220;
    --bg-2:         #111927;
    --bg-card:      #0f1824;
    --cyan:         #00e5ff;
    --cyan-dim:     rgba(0,229,255,0.10);
    --cyan-glow:    rgba(0,229,255,0.35);
    --amber:        #ffaa00;
    --green:        #00ff88;
    --red:          #ff4d6d;
    --text:         #dde8f5;
    --text-dim:     #4e6578;
    --border:       rgba(0,229,255,0.10);
    --border-hi:    rgba(0,229,255,0.35);
    --font-head:    'Syne', sans-serif;
    --font-mono:    'IBM Plex Mono', monospace;
}

/* ── Keyframes ──────────────────────────────────────────── */
@keyframes fadeUp {
    from { opacity:0; transform:translateY(18px); }
    to   { opacity:1; transform:translateY(0); }
}
@keyframes glowPulse {
    0%,100% { text-shadow: 0 0 18px var(--cyan-glow), 0 0 36px rgba(0,229,255,0.15); }
    50%     { text-shadow: 0 0 36px var(--cyan-glow), 0 0 72px rgba(0,229,255,0.30); }
}
@keyframes shimmer {
    from { background-position: -200% 0; }
    to   { background-position:  200% 0; }
}
@keyframes dotBlink {
    0%,100% { opacity:1; }
    50%     { opacity:0.25; }
}
@keyframes scanBeam {
    0%   { top:-2px; opacity:0; }
    5%   { opacity:1; }
    95%  { opacity:1; }
    100% { top:100%; opacity:0; }
}
@keyframes gridDrift {
    from { background-position: 0 0; }
    to   { background-position: 40px 40px; }
}
@keyframes borderGlow {
    0%,100% { box-shadow: 0 0 0 1px rgba(0,229,255,0.12); }
    50%     { box-shadow: 0 0 0 1px rgba(0,229,255,0.35), 0 0 20px rgba(0,229,255,0.08); }
}

/* ── App Background ─────────────────────────────────────── */
.stApp {
    background-color: var(--bg-0) !important;
    background-image:
        linear-gradient(rgba(0,229,255,0.025) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,229,255,0.025) 1px, transparent 1px);
    background-size: 40px 40px;
    animation: gridDrift 12s linear infinite;
    font-family: var(--font-mono) !important;
}

.main .block-container {
    padding-top: 1.5rem !important;
    max-width: 920px;
}

/* ── Hide Streamlit chrome ──────────────────────────────── */
#MainMenu, footer, .stDeployButton { visibility: hidden !important; }
header[data-testid="stHeader"] {
    background: rgba(6,11,23,0.8) !important;
    backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--border);
}

/* ── Sidebar ─────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 100%) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * {
    font-family: var(--font-mono) !important;
}
[data-testid="stSidebar"] hr {
    border-color: var(--border) !important;
    margin: 12px 0 !important;
}
[data-testid="stSidebar"] .stCaption {
    color: var(--text-dim) !important;
    font-size: 0.72rem !important;
}

/* ── Primary Buttons ─────────────────────────────────────── */
.stButton > button {
    background: transparent !important;
    color: var(--cyan) !important;
    border: 1px solid var(--border-hi) !important;
    border-radius: 3px !important;
    padding: 12px 24px !important;
    font-family: var(--font-mono) !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.10em !important;
    text-transform: uppercase !important;
    transition: all 0.2s ease !important;
    width: 100% !important;
}
.stButton > button:hover {
    background: var(--cyan-dim) !important;
    box-shadow: 0 0 20px rgba(0,229,255,0.18) !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="primary"] {
    background: var(--cyan) !important;
    color: var(--bg-0) !important;
    border-color: var(--cyan) !important;
    font-weight: 700 !important;
    box-shadow: 0 0 28px rgba(0,229,255,0.30) !important;
}
.stButton > button[kind="primary"]:hover {
    background: #33ecff !important;
    box-shadow: 0 0 44px rgba(0,229,255,0.50) !important;
    transform: translateY(-2px) !important;
}

/* ── Download Buttons ────────────────────────────────────── */
.stDownloadButton > button {
    background: transparent !important;
    color: var(--cyan) !important;
    border: 1px solid var(--border-hi) !important;
    border-radius: 3px !important;
    font-family: var(--font-mono) !important;
    font-size: 0.80rem !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    transition: all 0.2s ease !important;
    width: 100% !important;
}
.stDownloadButton > button[kind="primary"] {
    background: rgba(0,229,255,0.08) !important;
    box-shadow: 0 0 16px rgba(0,229,255,0.15) !important;
}
.stDownloadButton > button:hover {
    background: var(--cyan-dim) !important;
    box-shadow: 0 0 24px rgba(0,229,255,0.25) !important;
}

/* ── File Uploader ───────────────────────────────────────── */
[data-testid="stFileUploader"] {
    background: transparent !important;
    border: none !important;
}
[data-testid="stFileUploaderDropzoneInstructions"],
[data-testid="stFileUploaderDropzone"] {
    background: rgba(0,229,255,0.03) !important;
    border: 1px dashed var(--border-hi) !important;
    border-radius: 6px !important;
    transition: all 0.3s ease !important;
}
[data-testid="stFileUploaderDropzone"]:hover {
    background: rgba(0,229,255,0.06) !important;
    border-color: var(--cyan) !important;
    box-shadow: 0 0 28px rgba(0,229,255,0.12) !important;
}

/* ── Progress Bar ────────────────────────────────────────── */
[data-testid="stProgress"] > div > div > div > div {
    background: linear-gradient(90deg, var(--cyan), var(--green)) !important;
    box-shadow: 0 0 8px var(--cyan-glow) !important;
    border-radius: 0 !important;
}
[data-testid="stProgress"] > div > div {
    background: var(--bg-2) !important;
    border-radius: 0 !important;
}
[data-testid="stProgress"] p {
    font-family: var(--font-mono) !important;
    font-size: 0.75rem !important;
    color: var(--text-dim) !important;
    letter-spacing: 0.05em !important;
}

/* ── Dataframe ───────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    overflow: hidden;
    animation: borderGlow 4s ease-in-out infinite;
}

/* ── Alert boxes ─────────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 4px !important;
    font-family: var(--font-mono) !important;
    font-size: 0.82rem !important;
}
[data-testid="stInfo"] {
    background: rgba(0,229,255,0.05) !important;
    border-left: 2px solid var(--cyan) !important;
}
[data-testid="stSuccess"] {
    background: rgba(0,255,136,0.05) !important;
    border-left: 2px solid var(--green) !important;
}
[data-testid="stWarning"] {
    background: rgba(255,170,0,0.05) !important;
    border-left: 2px solid var(--amber) !important;
}
[data-testid="stError"] {
    background: rgba(255,77,109,0.05) !important;
    border-left: 2px solid var(--red) !important;
}

/* ── Spinner ─────────────────────────────────────────────── */
[data-testid="stSpinner"] > div {
    border-top-color: var(--cyan) !important;
}

/* ── Caption / small text ────────────────────────────────── */
.stCaption { color: var(--text-dim) !important; }

/* ── Custom layout components ────────────────────────────── */
.cf-logo {
    font-family: var(--font-head);
    font-size: 3.0rem;
    font-weight: 800;
    color: var(--cyan);
    text-align: center;
    letter-spacing: -0.02em;
    animation: glowPulse 3.5s ease-in-out infinite, fadeUp 0.5s ease both;
    line-height: 1;
    margin-bottom: 0;
}
.cf-logo em { color: var(--text); font-style: normal; }

.cf-tagline {
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    text-align: center;
    letter-spacing: 0.28em;
    text-transform: uppercase;
    animation: fadeUp 0.5s 0.18s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
    margin-top: 6px;
}

.cf-upload-box {
    position: relative;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 28px 28px 20px;
    margin-top: 20px;
    animation: fadeUp 0.5s 0.30s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
    overflow: hidden;
}
.cf-upload-box::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(135deg, rgba(0,229,255,0.04) 0%, transparent 55%);
    pointer-events: none;
}
.cf-upload-hint {
    font-family: var(--font-mono);
    font-size: 0.73rem;
    color: var(--text-dim);
    text-align: center;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    margin-bottom: 14px;
    display: block;
}

.cf-metric-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px 14px;
    text-align: center;
    position: relative;
    overflow: hidden;
    animation: fadeUp 0.45s ease both;
    transition: border-color 0.3s, box-shadow 0.3s;
}
.cf-metric-card:hover {
    border-color: var(--border-hi);
    box-shadow: 0 0 18px rgba(0,229,255,0.08);
}
.cf-metric-card::after {
    content: '';
    position: absolute;
    top: 0; left: -100%; width: 300%; height: 1px;
    background: linear-gradient(90deg, transparent, var(--cyan), transparent);
    animation: shimmer 3s ease-in-out infinite;
}
.cf-metric-icon  { font-size: 1.3rem; display: block; margin-bottom: 8px; }
.cf-metric-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-dim);
    letter-spacing: 0.18em;
    text-transform: uppercase;
    display: block;
    margin-bottom: 6px;
}
.cf-metric-value {
    font-family: var(--font-head);
    font-size: 1.55rem;
    font-weight: 700;
    color: var(--cyan);
    display: block;
    line-height: 1.15;
    word-break: break-all;
}
.cf-metric-value.sm {
    font-size: 0.85rem;
    color: var(--text);
    font-family: var(--font-mono);
    font-weight: 400;
}

.cf-rule {
    display: flex;
    align-items: center;
    gap: 10px;
    font-family: var(--font-mono);
    font-size: 0.70rem;
    color: var(--text-dim);
    letter-spacing: 0.18em;
    text-transform: uppercase;
    margin: 22px 0 12px;
}
.cf-rule::before { content:''; flex:none; width:14px; height:1px; background:var(--cyan); }
.cf-rule::after  { content:''; flex:1;    height:1px; background:var(--border); }

.cf-success-banner {
    background: var(--bg-card);
    border: 1px solid rgba(0,255,136,0.25);
    border-radius: 10px;
    padding: 36px 24px;
    text-align: center;
    position: relative;
    overflow: hidden;
    box-shadow: 0 0 48px rgba(0,255,136,0.06), inset 0 0 60px rgba(0,255,136,0.03);
    animation: fadeUp 0.5s ease both;
}
.cf-success-banner::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(135deg, rgba(0,255,136,0.05) 0%, transparent 55%);
    pointer-events: none;
}
.cf-success-icon  { font-size: 2.2rem; display: block; margin-bottom: 12px; }
.cf-success-title {
    font-family: var(--font-head);
    font-size: 1.75rem;
    font-weight: 800;
    color: var(--green);
    margin: 0 0 8px;
    letter-spacing: -0.01em;
}
.cf-success-sub {
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--text-dim);
    letter-spacing: 0.06em;
}

.cf-sidebar-brand {
    font-family: var(--font-head);
    font-size: 1.05rem;
    font-weight: 800;
    color: var(--cyan);
    letter-spacing: 0.04em;
    display: block;
    margin-bottom: 2px;
}
.cf-sidebar-desc {
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    line-height: 1.6;
}
.cf-issue-row {
    display: flex;
    align-items: center;
    gap: 8px;
    font-family: var(--font-mono);
    font-size: 0.74rem;
    color: var(--text-dim);
    margin: 5px 0;
}
.cf-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    flex: none;
    animation: dotBlink 2s ease-in-out infinite;
}
.cf-dot.high   { background: var(--red);   box-shadow: 0 0 5px rgba(255,77,109,0.7); }
.cf-dot.medium { background: var(--amber); box-shadow: 0 0 5px rgba(255,170,0,0.7); animation-delay:0.4s; }
.cf-dot.low    { background: var(--green); box-shadow: 0 0 5px rgba(0,255,136,0.7); animation-delay:0.8s; }

.cf-scan-title {
    font-family: var(--font-head);
    font-size: 1.9rem;
    font-weight: 800;
    color: var(--text);
    margin-bottom: 4px;
    animation: fadeUp 0.4s ease both;
    letter-spacing: -0.01em;
}
.cf-scan-sub {
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--text-dim);
    animation: fadeUp 0.4s 0.12s ease both;
    opacity: 0;
    animation-fill-mode: forwards;
}
.cf-scan-sub b { color: var(--cyan); font-weight: 500; }

.cf-sev-badge {
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 2px 7px;
    border-radius: 2px;
    font-weight: 600;
}
.cf-sev-badge.high   { color: var(--red);   background: rgba(255,77,109,0.12); border:1px solid rgba(255,77,109,0.3); }
.cf-sev-badge.medium { color: var(--amber); background: rgba(255,170,0,0.10);  border:1px solid rgba(255,170,0,0.3); }
.cf-sev-badge.low    { color: var(--green); background: rgba(0,255,136,0.08);  border:1px solid rgba(0,255,136,0.2); }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────

def show_sidebar():
    with st.sidebar:
        st.markdown(
            "<span class='cf-sidebar-brand'>⬡ CHAINFIX</span>"
            "<span class='cf-sidebar-desc'>AI-powered cleaner for messy supply chain data. "
            "Built for planners and operations teams dealing with dirty ERP exports.</span>",
            unsafe_allow_html=True,
        )
        st.divider()
        st.markdown("<div style='font-family:var(--font-mono);font-size:0.68rem;color:var(--text-dim);letter-spacing:0.18em;text-transform:uppercase;margin-bottom:10px;'>Supported Issues</div>", unsafe_allow_html=True)

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
        st.caption("Built by: Aditya")

        if not os.getenv("ANTHROPIC_API_KEY"):
            st.divider()
            st.warning(
                "ANTHROPIC_API_KEY is not set. "
                "Text standardization will use rule-based fallback instead of Claude AI.",
                icon="⚠️",
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
    report_filename = f"chainfix_report_{stem}_{ts}.xlsx"
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
        "<div class='cf-logo'>Chain<em>Fix</em></div>"
        "<div class='cf-tagline'>AI-Powered Supply Chain Data Cleaner</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

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
            "ChainFix will automatically fix all High and Medium severity issues. "
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
        "<div class='cf-success-sub'>All detectable problems have been fixed automatically.</div>"
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
    report_path = Path(st.session_state.get("report_path", str(DATA_OUTPUT / "chainfix_report.xlsx")))

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
                file_name="chainfix_report.xlsx",
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
        page_title="ChainFix",
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
