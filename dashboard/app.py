from pathlib import Path

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


def render_status(value: str) -> str:
    mapping = {
        "Escalate": "🔴 Escalate",
        "Alert": "🟠 Alert",
        "Watch": "🟡 Watch",
        "Ignore": "🟢 Ignore",
    }
    return mapping.get(value, value)


def load_data():
    summary_df = load_csv(SUMMARY_FILE)
    signals_df = load_csv(SIGNALS_FILE)
    history_df = load_csv(HISTORY_FILE)
    audit_df = load_csv(AUDIT_FILE)
    return summary_df, signals_df, history_df, audit_df


def main():
    from datetime import datetime
    summary_df, signals_df, history_df, audit_df = load_data()
    today_str = datetime.now().strftime("%B %d, %Y")
    st.title("📡 Supplier Watchtower")
    st.markdown(f"**Today's Date:** {today_str}")
    st.markdown("Welcome to our AI Agent for Supplier Evaluation and Risk Monitoring.")

    if not history_df.empty and "run_date" in history_df.columns:
        last_run = pd.to_datetime(history_df["run_date"], errors="coerce").max()
        if pd.notna(last_run):
            st.markdown(f"**Last Run Date:** {last_run.strftime('%B %d, %Y')}")
        else:
            st.markdown("No history available yet.")

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

    summary_df = summary_df.copy()
    signals_df = signals_df.copy()
    history_df = history_df.copy()
    audit_df = audit_df.copy()

    summary_df["status_display"] = summary_df["priority_level"].apply(render_status)
    summary_df["Score"] = (
        summary_df["total_risk_score"].astype(int).astype(str)
        + "/"
        + summary_df["max_possible_score"].astype(int).astype(str)
    )

    # Sidebar filters
    st.sidebar.header("Filters")

    category_options = ["All"] + sorted(summary_df["category_name"].dropna().unique().tolist())
    selected_category = st.sidebar.selectbox("Category", category_options)

    status_options = ["All"] + sorted(summary_df["priority_level"].dropna().unique().tolist())
    selected_status = st.sidebar.selectbox("Status", status_options)

    supplier_search = st.sidebar.text_input("Supplier search")

    filtered_summary = summary_df.copy()

    if selected_category != "All":
        filtered_summary = filtered_summary[filtered_summary["category_name"] == selected_category]

    if selected_status != "All":
        filtered_summary = filtered_summary[filtered_summary["priority_level"] == selected_status]

    if supplier_search:
        filtered_summary = filtered_summary[
            filtered_summary["supplier_name"].str.contains(supplier_search, case=False, na=False)
        ]

    filtered_supplier_names = filtered_summary["supplier_name"].dropna().unique().tolist()

    if not signals_df.empty:
        filtered_signals = signals_df[signals_df["supplier_name"].isin(filtered_supplier_names)]
    else:
        filtered_signals = signals_df.copy()

    # Executive summary
    st.subheader("Executive Summary")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Suppliers with Signals", len(filtered_summary))
    col2.metric("Total Signals", len(filtered_signals))
    col3.metric(
        "Alerts / Escalations",
        int(filtered_summary["priority_level"].isin(["Alert", "Escalate"]).sum())
    )
    col4.metric(
        "High Severity Signals",
        int((filtered_signals["severity"] == "High").sum()) if not filtered_signals.empty else 0
    )

    if not filtered_summary.empty:
        st.markdown("**Top 3 Suppliers to Watch**")
        for _, row in filtered_summary.sort_values(
            by=["total_risk_score", "event_count"], ascending=[False, False]
        ).head(3).iterrows():
            st.write(
                f"- **{row['supplier_name']}** | {row['priority_level']} | "
                f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])} — {row['risk_reason']}"
            )

    st.divider()

    # Charts
    left, right = st.columns(2)

    with left:
        st.subheader("Risk by Supplier")
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
        else:
            st.info("No supplier data available for this filter.")

    with right:
        st.subheader("Risk by Category")
        category_chart_df = (
            filtered_summary.groupby("category_name", as_index=False)
            .agg(total_risk=("total_risk_score", "sum"))
            .sort_values("total_risk", ascending=False)
        )
        if not category_chart_df.empty:
            fig = px.bar(
                category_chart_df,
                x="category_name",
                y="total_risk",
                title="Category Risk Totals",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data available.")

    st.subheader("Status Distribution")
    status_chart_df = (
        filtered_summary.groupby("priority_level", as_index=False)
        .agg(suppliers=("supplier_name", "count"))
    )
    if not status_chart_df.empty:
        fig = px.pie(
            status_chart_df,
            names="priority_level",
            values="suppliers",
            title="Supplier Status Distribution",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Supplier risk overview
    st.subheader("Supplier Risk Overview")
    overview_cols = [
        "supplier_name",
        "category_name",
        "Score",
        "event_count",
        "status_display",
        "risk_reason",
    ]
    overview_view = filtered_summary[overview_cols].rename(
        columns={
            "supplier_name": "Supplier",
            "category_name": "Category",
            "event_count": "Signals",
            "status_display": "Status",
            "risk_reason": "Why",
        }
    )
    st.dataframe(overview_view, use_container_width=True, hide_index=True)

    st.divider()

    # Recent signals
    st.subheader("Recent Signals")
    if not filtered_signals.empty:
        recent_cols = [
            "event_date",
            "supplier_name",
            "category_name",
            "signal_type",
            "severity",
            "source_title",
            "llm_reason",
            "recommended_action",
            "source_url",
        ]
        recent_view = filtered_signals[recent_cols].rename(
            columns={
                "event_date": "Date",
                "supplier_name": "Supplier",
                "category_name": "Category",
                "signal_type": "Signal Type",
                "severity": "Severity",
                "source_title": "Source Title",
                "llm_reason": "Why It Matters",
                "recommended_action": "Recommended Action",
                "source_url": "Source URL",
            }
        )
        st.dataframe(recent_view, use_container_width=True, hide_index=True)
    else:
        st.info("No recent signals available.")

    st.divider()

    # Supplier drilldown
    st.subheader("Supplier Drilldown")
    if filtered_supplier_names:
        selected_supplier = st.selectbox("Select supplier", filtered_supplier_names)

        supplier_row = filtered_summary[filtered_summary["supplier_name"] == selected_supplier].head(1)
        supplier_signals = filtered_signals[filtered_signals["supplier_name"] == selected_supplier].copy()

        if not supplier_row.empty:
            row = supplier_row.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Risk Score", f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])}")
            c2.metric("Signals", int(row["event_count"]))
            c3.metric("Status", row["priority_level"])

            st.markdown("**Risk Reason**")
            st.write(row["risk_reason"])

        if not supplier_signals.empty:
            detail_cols = [
                "event_date",
                "signal_type",
                "severity",
                "signal_text",
                "llm_reason",
                "recommended_action",
                "source_title",
                "source_url",
            ]
            st.dataframe(supplier_signals[detail_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No suppliers available for drilldown with current filters.")

    st.divider()

    # Risk history
    st.subheader("Risk History")
    if not history_df.empty:
        history_filtered = history_df[history_df["supplier_name"].isin(filtered_supplier_names)]
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
        hist_cols = [c for c in hist_cols if c in history_filtered.columns]
        st.dataframe(history_filtered[hist_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No history available yet.")

    st.divider()

    # Audit log
    st.subheader("Audit Log")
    if not audit_df.empty:
        audit_filtered = audit_df[audit_df["supplier_name"].isin(filtered_supplier_names)]
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
        st.dataframe(audit_filtered[audit_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No audit records available yet.")


if __name__ == "__main__":
    main()