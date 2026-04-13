# Tessergrid

> AI-Powered Data Cleaner for Any Spreadsheet

Tessergrid takes messy Excel and CSV files and automatically cleans them using AI. Upload any tabular data file — operations data, logistics records, inventory sheets, or any structured spreadsheet — and get back a clean file plus a detailed fix report.

---

## What It Does

Upload a messy Excel or CSV file. Tessergrid will:

1. **Scan** — detect all data quality problems automatically
2. **Clean** — fix everything using Claude AI
3. **Report** — give you a clear summary of every fix made
4. **Export** — download your clean, analysis-ready file

---

## Problems It Fixes

- Duplicate column names
- Mixed date formats
- Inconsistent country/region names
- Inconsistent status values
- Missing values
- Extra spaces in text fields
- Numbers stored as text
- Duplicate rows
- Empty junk rows
- Merged cells from ERP exports

---

## Tech Stack

- **Python** — core logic
- **Pandas** — Excel/CSV processing
- **Claude API** — AI-powered cleaning
- **Streamlit** — user interface

---

## Setup Instructions

### 1. Clone the project
```bash
git clone https://github.com/yourusername/chainfix.git
cd chainfix
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up your API key
```bash
cp .env.example .env
```
Then open `.env` and add your Anthropic API key:
```
ANTHROPIC_API_KEY=your_key_here
```

### 4. Run the app
```bash
streamlit run app/main.py
```

---

## Project Structure

```
chainfix/
├── src/          # Core logic (scanner, cleaner, reporter)
├── app/          # Streamlit frontend
├── data/         # Input, output, and sample files
└── tests/        # Test files
```

---

## Build Status

- [x] Project structure setup
- [ ] Scanner (detect problems)
- [ ] Cleaner (fix problems with Claude API)
- [ ] Reporter (generate fix summary)
- [ ] Streamlit frontend

---

Built by Aditya | Supply Chain x AI
