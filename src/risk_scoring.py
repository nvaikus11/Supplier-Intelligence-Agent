from pathlib import Path
import pandas as pd
import sys

from audit_logger import append_signal_audit_log, append_supplier_risk_history


SEVERITY_SCORE = {"Low": 1, "Medium": 2, "High": 3}
CONFIDENCE_SCORE = {"Low": 1, "Medium": 2, "High": 3}

SIGNAL_RISK_MAP = {
    "news": "supply_risk",
    "weather": "logistics_risk",
    "financial": "financial_risk",
    "esg_compliance": "esg_risk",
    "logistics": "logistics_risk",
    "supply_capacity": "supply_risk",
    "general_news": "other_risk",
}

MAX_SIGNALS_PER_SUPPLIER = 3
MAX_EVENT_SCORE = 9
MAX_TOTAL_SCORE = MAX_SIGNALS_PER_SUPPLIER * MAX_EVENT_SCORE


def load_signals(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Signals file not found: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

    return df


def score_signals(signals_df: pd.DataFrame) -> pd.DataFrame:
    df = signals_df.copy()

    df["severity_score"] = df["severity"].map(SEVERITY_SCORE).fillna(1)
    df["confidence_score"] = df["confidence"].map(CONFIDENCE_SCORE).fillna(1)
    df["base_risk_score"] = df["severity_score"] * df["confidence_score"]
    df["risk_bucket"] = df["signal_type"].map(SIGNAL_RISK_MAP).fillna("other_risk")

    return df


def assign_priority(score: int) -> str:
    if score >= 19:
        return "Escalate"
    if score >= 11:
        return "Alert"
    if score >= 6:
        return "Watch"
    return "Ignore"


def build_risk_reason(supplier_events: pd.DataFrame, total_score: int) -> str:
    if supplier_events.empty or total_score == 0:
        return "No material public risk signals detected today."

    ranked = supplier_events.sort_values(
        by=["base_risk_score", "severity_score", "confidence_score"],
        ascending=[False, False, False],
    )

    top_event = ranked.iloc[0]
    top_reason = str(top_event.get("llm_reason", "")).strip()

    if top_reason:
        return top_reason

    signal_type = str(top_event.get("signal_type", "general_news"))
    severity = str(top_event.get("severity", "Low"))

    if severity == "High":
        return f"Highlighted due to a high-severity {signal_type} signal."
    if severity == "Medium":
        return f"Highlighted due to a medium-risk {signal_type} signal."
    return f"Low-level {signal_type} signals were detected and are being monitored."


def aggregate_supplier_risk(scored_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        scored_df.groupby(["supplier_id", "supplier_name", "category_id"], as_index=False)
        .agg(
            total_risk_score=("base_risk_score", "sum"),
            event_count=("event_id", "count"),
            high_severity_events=("severity", lambda x: (x == "High").sum()),
        )
    )

    if "category_name" in scored_df.columns:
        category_name_lookup = (
            scored_df[["supplier_id", "category_name"]]
            .dropna()
            .drop_duplicates(subset=["supplier_id"])
        )
        grouped = grouped.merge(category_name_lookup, on="supplier_id", how="left")
    else:
        grouped["category_name"] = ""

    grouped["max_possible_score"] = MAX_TOTAL_SCORE
    grouped["priority_level"] = grouped["total_risk_score"].apply(assign_priority)

    risk_reasons = []
    for _, supplier_row in grouped.iterrows():
        supplier_events = scored_df[scored_df["supplier_id"] == supplier_row["supplier_id"]]
        reason = build_risk_reason(supplier_events, int(supplier_row["total_risk_score"]))
        risk_reasons.append(reason)

    grouped["risk_reason"] = risk_reasons

    return grouped.sort_values(
        by=["total_risk_score", "high_severity_events", "event_count"],
        ascending=[False, False, False],
    )


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    input_file = base_dir / "outputs" / "daily_reports" / "real_signals.csv"
    scored_output_file = base_dir / "outputs" / "daily_reports" / "scored_signals.csv"
    summary_output_file = base_dir / "outputs" / "daily_reports" / "supplier_risk_summary.csv"

    signals = load_signals(input_file)
    if signals.empty:
        print("\nNo signals found for this run. Skipping scoring.\n")
        sys.exit(0)
    scored_signals = score_signals(signals)
    supplier_summary = aggregate_supplier_risk(scored_signals)

    scored_signals.to_csv(scored_output_file, index=False)
    supplier_summary.to_csv(summary_output_file, index=False)

    append_signal_audit_log(scored_signals)
    append_supplier_risk_history(supplier_summary)

    print("\nScored signals preview:\n")
    print(scored_signals.head(10).to_string(index=False))

    print("\nSupplier risk summary:\n")
    print(supplier_summary.head(10).to_string(index=False))

    print(f"\nSaved scored signals to: {scored_output_file}")
    print(f"Saved supplier summary to: {summary_output_file}")
    print(f"Updated audit log: {base_dir / 'outputs' / 'logs' / 'signal_audit_log.csv'}")
    print(f"Updated risk history: {base_dir / 'outputs' / 'history' / 'supplier_risk_history.csv'}")