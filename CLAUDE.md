# ChainFix

AI-powered supply chain data cleaning tool. Users upload messy Excel/CSV files — the tool scans for problems, uses the Claude API to fix them, and outputs a clean file plus a summary report.

## Tech Stack
- Python + Pandas
- Claude API (Anthropic)
- Streamlit (frontend)
- openpyxl (Excel handling)

## Folder Structure
```
chainfix/
├── CLAUDE.md              # This file
├── .claude/               # Claude Code configuration and context files
│   ├── settings.json
│   ├── scanner_context.md
│   ├── cleaner_context.md
│   ├── api_context.md
│   └── domain/
│       ├── by_glossary.md
│       ├── by_tables.md
│       ├── by_planning.md
│       └── client_rules.md
├── data/
│   ├── input/             # Raw messy files uploaded by user
│   ├── output/            # Cleaned files ready to download
│   └── samples/           # Sample test files for development
├── src/
│   ├── scanner.py
│   ├── cleaner.py
│   ├── reporter.py
│   └── utils.py
├── app/
│   └── main.py
└── tests/
    └── test_scanner.py
```

## How to Load Context

Before writing any code, read the relevant context file:

| Task | Read this file |
|------|---------------|
| Building or modifying scanner.py | `.claude/scanner_context.md` |
| Building or modifying cleaner.py | `.claude/cleaner_context.md` |
| API keys, security, or Claude API calls | `.claude/api_context.md` |
| Blue Yonder column terminology | `.claude/domain/by_glossary.md` |
| Blue Yonder table structures | `.claude/domain/by_tables.md` |
| Blue Yonder planning logic | `.claude/domain/by_planning.md` |
| Client-specific rules | `.claude/domain/client_rules.md` |
