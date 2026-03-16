# Supplier Watchtower

AI-powered Supplier Evaluation Copilot / Watchtower / Workflow Engine for Global Supply Management.

This project monitors a defined list of suppliers, discovers relevant public-source pages, uses LLM reasoning to assess supplier risk, scores the signals, generates a daily brief, and sends a concise alert through OpenClaw + BlueBubbles.

---

## What it does

For a selected supplier set, the system answers:

- What changed?
- What matters?
- Who should I care about today?
- What action should I take now?
- Who should be nudged automatically?

It currently supports:

- Static vendor master file
- Browser-first page discovery
- Full-page text extraction
- LLM-based signal relevance + severity reasoning
- Risk scoring
- Daily brief generation
- Audit log and risk history
- BlueBubbles alert delivery
- Streamlit dashboard

---

## Current test setup

The system is currently configured in **test mode**:

- 1 vendor per category
- 4 vendors total
- up to 8 pages per vendor

Categories:
- Camera Lens
- Sound Components
- Packaging
- Titanium

---

## Project structure

```text
supplier-watchtower/
│
├── data/
│   └── vendor_master.xlsx
│
├── src/
│   ├── load_vendor_master.py
│   ├── collect_signals.py
│   ├── browser_research.py
│   ├── browser_enrich_and_score.py
│   ├── llm_signal_extractor.py
│   ├── risk_scoring.py
│   ├── audit_logger.py
│   ├── generate_daily_brief.py
│   ├── send_watchtower_alert.py
│   └── run_and_alert_watchtower.py
│
├── configs/
│   └── category_keywords.yaml
│
├── dashboard/
│   └── app.py
│
├── outputs/
│   ├── daily_reports/
│   ├── history/
│   └── logs/
│
└── README.md