from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent.parent
SUMMARY_FILE = BASE_DIR / "outputs" / "daily_reports" / "supplier_risk_summary.csv"
SIGNALS_FILE = BASE_DIR / "outputs" / "daily_reports" / "scored_signals.csv"
HISTORY_FILE = BASE_DIR / "outputs" / "history" / "supplier_risk_history.csv"
AUDIT_FILE = BASE_DIR / "outputs" / "logs" / "signal_audit_log.csv"


st.set_page_config(
    page_title="Supplier Watchtower",
    page_icon="📡",
    layout="wide",
)


@st.cache_data
def load_csv(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except Exception:
        return pd.DataFrame()


def load_data():
    summary_df = load_csv(SUMMARY_FILE)
    signals_df = load_csv(SIGNALS_FILE)
    history_df = load_csv(HISTORY_FILE)
    audit_df = load_csv(AUDIT_FILE)
    return summary_df, signals_df, history_df, audit_df


def render_status(value: str) -> str:
    mapping = {
        "Escalate": "🔴 Escalate",
        "Alert": "🟠 Alert",
        "Watch": "🟡 Watch",
        "Ignore": "🟢 Ignore",
    }
    return mapping.get(value, value)


def get_score_band(score: float) -> str:
    if score >= 19:
        return "Escalate"
    if score >= 11:
        return "Alert"
    if score >= 6:
        return "Watch"
    return "Ignore"


def get_score_interpretation(score: float) -> str:
    if score >= 19:
        return "Multiple strong, credible signals. Immediate escalation recommended."
    if score >= 11:
        return "Meaningful supplier risk. Action or follow-up is likely needed."
    if score >= 6:
        return "Moderate risk signal. Review and monitor closely."
    if score >= 1:
        return "Minor but credible signal. Log and continue monitoring."
    return "No material public risk signals detected."


def enrich_data(summary_df: pd.DataFrame, signals_df: pd.DataFrame, history_df: pd.DataFrame):
    if not summary_df.empty:
        summary_df = summary_df.copy()
        summary_df["status_display"] = summary_df["priority_level"].apply(render_status)
        summary_df["Score"] = (
            summary_df["total_risk_score"].astype(int).astype(str)
            + "/"
            + summary_df["max_possible_score"].astype(int).astype(str)
        )

    if not signals_df.empty:
        signals_df = signals_df.copy()
        if "severity_score" not in signals_df.columns:
            severity_map = {"Low": 1, "Medium": 2, "High": 3}
            signals_df["severity_score"] = signals_df["severity"].map(severity_map).fillna(1)
        if "confidence_score" not in signals_df.columns:
            confidence_map = {"Low": 1, "Medium": 2, "High": 3}
            signals_df["confidence_score"] = signals_df["confidence"].map(confidence_map).fillna(1)
        if "base_risk_score" not in signals_df.columns:
            signals_df["base_risk_score"] = signals_df["severity_score"] * signals_df["confidence_score"]

    if not history_df.empty and "run_date" in history_df.columns:
        history_df = history_df.copy()
        history_df["run_date"] = pd.to_datetime(history_df["run_date"], errors="coerce")

    return summary_df, signals_df, history_df


def apply_filters(summary_df: pd.DataFrame, signals_df: pd.DataFrame, history_df: pd.DataFrame, audit_df: pd.DataFrame):
    st.sidebar.header("Filters")

    category_options = ["All"] + sorted(summary_df["category_name"].dropna().unique().tolist()) if not summary_df.empty else ["All"]
    selected_category = st.sidebar.selectbox("Category", category_options)

    status_options = ["All"] + sorted(summary_df["priority_level"].dropna().unique().tolist()) if not summary_df.empty else ["All"]
    selected_status = st.sidebar.selectbox("Status", status_options)

    supplier_search = st.sidebar.text_input("Supplier search")

    filtered_summary = summary_df.copy()

    if not filtered_summary.empty:
        if selected_category != "All":
            filtered_summary = filtered_summary[filtered_summary["category_name"] == selected_category]

        if selected_status != "All":
            filtered_summary = filtered_summary[filtered_summary["priority_level"] == selected_status]

        if supplier_search:
            filtered_summary = filtered_summary[
                filtered_summary["supplier_name"].str.contains(supplier_search, case=False, na=False)
            ]

    filtered_supplier_names = filtered_summary["supplier_name"].dropna().unique().tolist() if not filtered_summary.empty else []

    filtered_signals = signals_df[signals_df["supplier_name"].isin(filtered_supplier_names)] if not signals_df.empty else signals_df
    filtered_history = history_df[history_df["supplier_name"].isin(filtered_supplier_names)] if not history_df.empty and filtered_supplier_names else history_df
    filtered_audit = audit_df[audit_df["supplier_name"].isin(filtered_supplier_names)] if not audit_df.empty and filtered_supplier_names else audit_df

    return filtered_summary, filtered_signals, filtered_history, filtered_audit, filtered_supplier_names


def methodology_section():
    st.subheader("How Risk Scoring Works")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### 1. Event-Level Risk Score")
        st.markdown("Each detected signal gets an **event risk score**:")
        st.code("Event Risk Score = Severity Score × Confidence Score")

        severity_df = pd.DataFrame({
            "Severity": ["Low", "Medium", "High"],
            "Score": [1, 2, 3],
            "Meaning": [
                "Minor / informational signal",
                "Notable signal that may need review",
                "Material signal with potential supply impact",
            ],
        })
        st.dataframe(severity_df, use_container_width=True, hide_index=True)

    with c2:
        st.markdown("### 2. Confidence Score")
        st.markdown("Confidence measures **how much we trust the signal**.")
        confidence_df = pd.DataFrame({
            "Confidence": ["Low", "Medium", "High"],
            "Score": [1, 2, 3],
            "Meaning": [
                "Weak or unclear evidence",
                "Some evidence from usable sources",
                "Strong evidence from credible sources",
            ],
        })
        st.dataframe(confidence_df, use_container_width=True, hide_index=True)

    st.markdown("### 3. Supplier-Level Risk Score")
    st.markdown("Supplier score is the **sum of event scores** for that supplier.")
    st.code("Supplier Risk Score = Sum of Event Risk Scores")

    score_guide = pd.DataFrame({
        "Raw Score Range": ["0", "1–5", "6–10", "11–18", "19+"],
        "Status": ["Ignore", "Ignore", "Watch", "Alert", "Escalate"],
        "Interpretation": [
            "No material public risk signals detected",
            "Minor signal, log and monitor",
            "Moderate risk, review closely",
            "Meaningful risk, action likely needed",
            "Multiple strong signals, escalate now",
        ],
        "Suggested Response": [
            "Continue monitoring",
            "Log only",
            "Review",
            "Take action",
            "Immediate escalation",
        ],
    })
    st.dataframe(score_guide, use_container_width=True, hide_index=True)

    st.markdown("### 4. What does 3 vs 9 vs 15 mean?")
    explain_df = pd.DataFrame({
        "Example Score": [3, 9, 15],
        "Typical Meaning": [
            "One low-severity but credible signal",
            "One very strong signal or several moderate ones",
            "Multiple meaningful signals reinforcing risk",
        ],
        "Business Interpretation": [
            "Worth logging, not urgent",
            "Real concern, should review",
            "Elevated risk, action likely needed",
        ],
    })
    st.dataframe(explain_df, use_container_width=True, hide_index=True)

    st.markdown("### 5. Severity vs Confidence")
    matrix_df = pd.DataFrame({
        "Severity \\ Confidence": ["Low", "Medium", "High"],
        "Low": [1, 2, 3],
        "Medium": [2, 4, 6],
        "High": [3, 6, 9],
    })
    st.dataframe(matrix_df, use_container_width=True, hide_index=True)


def supplier_breakdown_section(selected_supplier: str, summary_df: pd.DataFrame, signals_df: pd.DataFrame, chart_key_suffix: str = "default"):
    if not selected_supplier:
        st.info("Select a supplier to see score breakdown.")
        return

    supplier_row = summary_df[summary_df["supplier_name"] == selected_supplier].head(1)
    supplier_signals = signals_df[signals_df["supplier_name"] == selected_supplier].copy()

    if supplier_row.empty:
        st.info("No summary found for the selected supplier.")
        return

    row = supplier_row.iloc[0]

    st.markdown(f"### Score Breakdown for {selected_supplier}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Raw Score", f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])}")
    c2.metric("Status", row["priority_level"])
    c3.metric("Signals", int(row["event_count"]))
    c4.metric("High Severity Events", int(row["high_severity_events"]))

    st.markdown("**Why this score?**")
    st.write(row["risk_reason"])
    st.info(get_score_interpretation(row["total_risk_score"]))

    if supplier_signals.empty:
        st.info("No contributing signals found.")
        return

    breakdown_cols = [
        "event_date",
        "signal_type",
        "severity",
        "confidence",
        "severity_score",
        "confidence_score",
        "base_risk_score",
        "llm_reason",
        "recommended_action",
        "source_title",
        "source_url",
    ]
    breakdown_cols = [c for c in breakdown_cols if c in supplier_signals.columns]

    pretty_breakdown = supplier_signals[breakdown_cols].rename(columns={
        "event_date": "Date",
        "signal_type": "Signal Type",
        "severity": "Severity",
        "confidence": "Confidence",
        "severity_score": "Severity Score",
        "confidence_score": "Confidence Score",
        "base_risk_score": "Event Score",
        "llm_reason": "Why It Matters",
        "recommended_action": "Recommended Action",
        "source_title": "Source Title",
        "source_url": "Source URL",
    })

    st.dataframe(pretty_breakdown, use_container_width=True, hide_index=True)

    chart_df = supplier_signals.copy()
    chart_df["label"] = chart_df["signal_type"] + " | " + chart_df["severity"]
    fig = px.bar(
        chart_df,
        x="label",
        y="base_risk_score",
        color="signal_type",
        title=f"Event Score Contribution - {selected_supplier}",
    )
    st.plotly_chart(
    fig,
    use_container_width=True,
    key=f"event_score_chart_{selected_supplier}_{chart_key_suffix}"
)


def main():
    summary_df, signals_df, history_df, audit_df = load_data()

    st.title("📡 Supplier Watchtower")
    today_str = datetime.now().strftime("%B %d, %Y")
    st.markdown(f"**Today's Date:** {today_str}")
    st.markdown("Welcome to our AI Agent for Supplier Evaluation and Risk Monitoring.")

    if not history_df.empty and "run_date" in history_df.columns:
        last_run = pd.to_datetime(history_df["run_date"], errors="coerce").max()
        if pd.notna(last_run):
            st.markdown(f"**Last Run Date:** {last_run.strftime('%B %d, %Y')}")

    if summary_df.empty:
        st.warning("No supplier summary available yet. Run the pipeline first.")
        st.code(
            "python src/browser_research.py\n"
            "python src/browser_enrich_and_score.py\n"
            "python src/risk_scoring.py\n"
            "python src/generate_daily_brief.py\n"
            "python src/send_watchtower_alert.py"
        )
        return

    summary_df, signals_df, history_df = enrich_data(summary_df, signals_df, history_df)
    filtered_summary, filtered_signals, filtered_history, filtered_audit, filtered_supplier_names = apply_filters(
        summary_df, signals_df, history_df, audit_df
    )

    tabs = st.tabs([
        "Executive Overview",
        "Supplier Drilldown",
        "Signals & Evidence",
        "Risk Methodology",
        "Audit & History",
    ])

    with tabs[0]:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Suppliers with Signals", len(filtered_summary))
        col2.metric("Total Signals", len(filtered_signals))
        col3.metric(
            "Alerts / Escalations",
            int(filtered_summary["priority_level"].isin(["Alert", "Escalate"]).sum()) if not filtered_summary.empty else 0,
        )
        col4.metric(
            "High Severity Signals",
            int((filtered_signals["severity"] == "High").sum()) if not filtered_signals.empty else 0,
        )

        st.markdown("### Top Suppliers to Watch")
        if not filtered_summary.empty:
            for _, row in filtered_summary.sort_values(
                by=["total_risk_score", "event_count"], ascending=[False, False]
            ).head(3).iterrows():
                st.write(
                    f"- **{row['supplier_name']}** | {row['priority_level']} | "
                    f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])} — {row['risk_reason']}"
                )

        left, right = st.columns(2)

        with left:
            st.markdown("### Risk by Supplier")
            chart_df = filtered_summary.sort_values("total_risk_score", ascending=False).head(10)
            if not chart_df.empty:
                fig = px.bar(
                    chart_df,
                    x="supplier_name",
                    y="total_risk_score",
                    color="priority_level",
                    title="Top Supplier Risk Scores",
                )
                st.plotly_chart(fig, use_container_width=True)

        with right:
            st.markdown("### Risk by Category")
            if not filtered_summary.empty:
                category_chart_df = (
                    filtered_summary.groupby("category_name", as_index=False)
                    .agg(total_risk=("total_risk_score", "sum"))
                    .sort_values("total_risk", ascending=False)
                )
                fig = px.bar(
                    category_chart_df,
                    x="category_name",
                    y="total_risk",
                    title="Category Risk Totals",
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Supplier Risk Overview")
        if not filtered_summary.empty:
            overview_cols = [
                "supplier_name",
                "category_name",
                "Score",
                "event_count",
                "status_display",
                "risk_reason",
            ]
            overview_view = filtered_summary[overview_cols].rename(columns={
                "supplier_name": "Supplier",
                "category_name": "Category",
                "event_count": "Signals",
                "status_display": "Status",
                "risk_reason": "Why",
            })
            st.dataframe(overview_view, use_container_width=True, hide_index=True)

    with tabs[1]:
        selected_supplier = st.selectbox(
            "Select supplier",
            filtered_supplier_names if filtered_supplier_names else ["No suppliers available"],
        )

        if filtered_supplier_names:
            supplier_breakdown_section(
                selected_supplier,
                filtered_summary,
                filtered_signals,
                chart_key_suffix="drilldown"
            )

    with tabs[2]:
        st.markdown("### Recent Signals & Evidence")
        if not filtered_signals.empty:
            recent_cols = [
                "event_date",
                "supplier_name",
                "category_name",
                "signal_type",
                "severity",
                "confidence",
                "base_risk_score",
                "source_title",
                "llm_reason",
                "recommended_action",
                "source_url",
            ]
            recent_cols = [c for c in recent_cols if c in filtered_signals.columns]
            recent_view = filtered_signals[recent_cols].rename(columns={
                "event_date": "Date",
                "supplier_name": "Supplier",
                "category_name": "Category",
                "signal_type": "Signal Type",
                "severity": "Severity",
                "confidence": "Confidence",
                "base_risk_score": "Event Score",
                "source_title": "Source Title",
                "llm_reason": "Why It Matters",
                "recommended_action": "Recommended Action",
                "source_url": "Source URL",
            })
            st.dataframe(recent_view, use_container_width=True, hide_index=True)
        else:
            st.info("No recent signals available.")

    with tabs[3]:
        methodology_section()
        st.divider()
        if filtered_supplier_names:
            st.markdown("### Supplier-Specific Methodology View")
            methodology_supplier = st.selectbox(
                "Choose supplier for score explanation",
                filtered_supplier_names,
                key="method_supplier",
            )
            supplier_breakdown_section(
                methodology_supplier,
                filtered_summary,
                filtered_signals,
                chart_key_suffix="methodology"
            )

    with tabs[4]:
        st.markdown("### Risk History")
        if not filtered_history.empty:
            hist_cols = [
                "run_date",
                "supplier_name",
                "category_name",
                "total_risk_score",
                "max_possible_score",
                "priority_level",
                "event_count",
                "risk_reason",
            ]
            hist_cols = [c for c in hist_cols if c in filtered_history.columns]
            st.dataframe(filtered_history[hist_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No history available yet.")

        st.markdown("### Audit Log")
        if not filtered_audit.empty:
            audit_cols = [
                "logged_at",
                "supplier_name",
                "category_name",
                "signal_type",
                "severity",
                "base_risk_score",
                "source_title",
                "source_url",
                "llm_reason",
                "recommended_action",
            ]
            audit_cols = [c for c in audit_cols if c in filtered_audit.columns]
            st.dataframe(filtered_audit[audit_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No audit records available yet.")


if __name__ == "__main__":
    main()