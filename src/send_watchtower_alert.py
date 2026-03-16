from pathlib import Path
import subprocess
import sys
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
SUMMARY_FILE = BASE_DIR / "outputs" / "daily_reports" / "supplier_risk_summary.csv"
SCORED_SIGNALS_FILE = BASE_DIR / "outputs" / "daily_reports" / "scored_signals.csv"
DASHBOARD_URL = "http://127.0.0.1:8501"
TARGET_PHONE = "+14704180733"


def load_data():
    if not SUMMARY_FILE.exists():
        raise FileNotFoundError(f"Missing file: {SUMMARY_FILE}")
    if not SCORED_SIGNALS_FILE.exists():
        raise FileNotFoundError(f"Missing file: {SCORED_SIGNALS_FILE}")

    summary_df = pd.read_csv(SUMMARY_FILE)
    signals_df = pd.read_csv(SCORED_SIGNALS_FILE)
    return summary_df, signals_df


def build_alert_message(summary_df: pd.DataFrame, signals_df: pd.DataFrame) -> str:
    top_suppliers = summary_df.sort_values(
        by=["total_risk_score", "high_severity_events", "event_count"],
        ascending=[False, False, False],
    ).head(2)

    if top_suppliers.empty:
        return f"Supplier Watchtower\n\nNo material supplier risk signals detected today.\n\nDashboard: {DASHBOARD_URL}"

    message = ["Supplier Watchtower", "\nTop Risks"]

    for i, row in enumerate(top_suppliers.itertuples(), 1):
        supplier_signals = signals_df[
            signals_df["supplier_name"] == row.supplier_name
        ].sort_values(
            by=["base_risk_score", "severity_score"],
            ascending=[False, False],
        )

        evidence_url = supplier_signals.iloc[0]["source_url"] if len(supplier_signals) else ""

        message.append(
            f"{i}. {row.supplier_name} | {row.priority_level} | "
            f"{int(row.total_risk_score)}/{int(row.max_possible_score)}"
        )
        message.append(f"   {row.risk_reason}")

        if evidence_url:
            message.append(f"   {evidence_url}")

    message.append("\nDashboard")
    message.append(DASHBOARD_URL)

    return "\n".join(message)[:3000]


def send_bluebubbles_message(message: str, target: str = TARGET_PHONE) -> None:
    command = [
        "openclaw",
        "message",
        "send",
        "--channel",
        "bluebubbles",
        "--target",
        target,
        "--message",
        message,
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print("Failed to send BlueBubbles message.")
        print(result.stderr)
        sys.exit(result.returncode)

    print("BlueBubbles message sent successfully.")
    print(result.stdout)


if __name__ == "__main__":
    summary_df, signals_df = load_data()
    alert_message = build_alert_message(summary_df, signals_df)

    print("\nSending this alert:\n")
    print(alert_message)
    print("\n---\n")

    send_bluebubbles_message(alert_message)