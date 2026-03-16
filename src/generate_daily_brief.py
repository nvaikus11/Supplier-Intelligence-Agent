from pathlib import Path
from datetime import datetime
import pandas as pd


def load_inputs(signals_file: Path, summary_file: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not signals_file.exists():
        raise FileNotFoundError(f"Signals file not found: {signals_file}")
    if not summary_file.exists():
        raise FileNotFoundError(f"Supplier summary file not found: {summary_file}")

    signals_df = pd.read_csv(signals_file)
    summary_df = pd.read_csv(summary_file)
    return signals_df, summary_df


def get_what_changed(signals_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    return (
        signals_df.sort_values(
            by=["severity_score", "confidence_score", "event_date"],
            ascending=[False, False, False],
        )[
            [
                "event_date",
                "supplier_name",
                "signal_type",
                "signal_text",
                "severity",
                "confidence",
            ]
        ]
        .head(top_n)
    )


def get_what_matters(summary_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    return summary_df[
        [
            "supplier_name",
            "category_id",
            "priority_level",
            "total_risk_score",
            "max_possible_score",
            "risk_reason",
        ]
    ].head(top_n)


def get_who_to_care(summary_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    return summary_df[
        [
            "supplier_name",
            "category_id",
            "priority_level",
            "total_risk_score",
            "max_possible_score",
            "risk_reason",
        ]
    ].head(top_n)


def get_recommended_actions(signals_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    return (
        signals_df.sort_values(
            by=["severity_score", "confidence_score"],
            ascending=[False, False],
        )[
            [
                "supplier_name",
                "signal_type",
                "severity",
                "recommended_action",
            ]
        ]
        .drop_duplicates()
        .head(top_n)
    )


def get_auto_nudges(summary_df: pd.DataFrame) -> list[str]:
    nudges = []
    high_priority = summary_df[summary_df["priority_level"] == "High"]

    for _, row in high_priority.iterrows():
        nudges.append(
            f"Nudge category manager for {row['supplier_name']} "
            f"(score: {int(row['total_risk_score'])}/{int(row['max_possible_score'])})"
        )

    if not nudges:
        nudges.append("No high-priority nudges today.")

    return nudges


def write_brief(
    output_file: Path,
    what_changed: pd.DataFrame,
    what_matters: pd.DataFrame,
    who_to_care: pd.DataFrame,
    actions: pd.DataFrame,
    nudges: list[str],
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("SUPPLIER WATCHTOWER DAILY BRIEF\n")
        f.write("=" * 40 + "\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("1. WHAT CHANGED?\n")
        f.write("-" * 20 + "\n")
        f.write(what_changed.to_string(index=False))
        f.write("\n\n")

        f.write("2. WHAT MATTERS?\n")
        f.write("-" * 20 + "\n")
        for _, row in what_matters.iterrows():
            f.write(
                f"- {row['supplier_name']} | {row['priority_level']} risk | "
                f"Score {int(row['total_risk_score'])}/{int(row['max_possible_score'])}\n"
            )
            f.write(f"  Why: {row['risk_reason']}\n")
        f.write("\n")

        f.write("3. WHO SHOULD I CARE ABOUT TODAY?\n")
        f.write("-" * 20 + "\n")
        for _, row in who_to_care.iterrows():
            f.write(
                f"- {row['supplier_name']} | {row['priority_level']} risk | "
                f"Score {int(row['total_risk_score'])}/{int(row['max_possible_score'])}\n"
            )
            f.write(f"  Why: {row['risk_reason']}\n")
        f.write("\n")

        f.write("4. WHAT ACTION SHOULD I TAKE NOW?\n")
        f.write("-" * 20 + "\n")
        for _, row in actions.iterrows():
            f.write(
                f"- {row['supplier_name']} | {row['signal_type']} | "
                f"{row['severity']}\n"
            )
            f.write(f"  Action: {row['recommended_action']}\n")
        f.write("\n")

        f.write("5. WHO SHOULD BE NUDGED AUTOMATICALLY?\n")
        f.write("-" * 20 + "\n")
        for nudge in nudges:
            f.write(f"- {nudge}\n")


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    signals_file = base_dir / "outputs" / "daily_reports" / "scored_signals.csv"
    summary_file = base_dir / "outputs" / "daily_reports" / "supplier_risk_summary.csv"
    brief_file = base_dir / "outputs" / "daily_reports" / "daily_brief.txt"

    signals_df, summary_df = load_inputs(signals_file, summary_file)

    what_changed = get_what_changed(signals_df)
    what_matters = get_what_matters(summary_df)
    who_to_care = get_who_to_care(summary_df)
    actions = get_recommended_actions(signals_df)
    nudges = get_auto_nudges(summary_df)

    write_brief(
        brief_file,
        what_changed,
        what_matters,
        who_to_care,
        actions,
        nudges,
    )

    print(f"\nDaily brief generated successfully: {brief_file}\n")
    print(brief_file.read_text(encoding="utf-8"))