from pathlib import Path
from datetime import datetime
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
AUDIT_LOG_FILE = BASE_DIR / "outputs" / "logs" / "signal_audit_log.csv"
RISK_HISTORY_FILE = BASE_DIR / "outputs" / "history" / "supplier_risk_history.csv"


def append_signal_audit_log(scored_signals: pd.DataFrame) -> None:
    AUDIT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    audit_df = scored_signals.copy()
    audit_df["logged_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    columns = [
        "logged_at",
        "event_date",
        "supplier_id",
        "supplier_name",
        "category_id",
        "category_name",
        "signal_type",
        "severity",
        "confidence",
        "base_risk_score",
        "source_type",
        "source_title",
        "source_url",
        "signal_text",
        "llm_reason",
        "recommended_action",
    ]

    for col in columns:
        if col not in audit_df.columns:
            audit_df[col] = ""

    audit_df = audit_df[columns]

    if AUDIT_LOG_FILE.exists():
        existing = pd.read_csv(AUDIT_LOG_FILE)
        audit_df = pd.concat([existing, audit_df], ignore_index=True)

    audit_df.to_csv(AUDIT_LOG_FILE, index=False)


def append_supplier_risk_history(supplier_summary: pd.DataFrame) -> None:
    RISK_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    history_df = supplier_summary.copy()
    history_df["run_date"] = datetime.now().strftime("%Y-%m-%d")

    columns = [
        "run_date",
        "supplier_id",
        "supplier_name",
        "category_id",
        "total_risk_score",
        "max_possible_score",
        "priority_level",
        "event_count",
        "high_severity_events",
        "risk_reason",
    ]

    for col in columns:
        if col not in history_df.columns:
            history_df[col] = ""

    history_df = history_df[columns]

    if RISK_HISTORY_FILE.exists():
        existing = pd.read_csv(RISK_HISTORY_FILE)
        history_df = pd.concat([existing, history_df], ignore_index=True)

    history_df.to_csv(RISK_HISTORY_FILE, index=False)