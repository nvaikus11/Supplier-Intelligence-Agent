
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent.parent
SUMMARY_FILE = BASE_DIR / "outputs" / "daily_reports" / "supplier_risk_summary.csv"
SIGNALS_FILE = BASE_DIR / "outputs" / "daily_reports" / "scored_signals.csv"
HISTORY_FILE = BASE_DIR / "outputs" / "history" / "supplier_risk_history.csv"
AUDIT_FILE = BASE_DIR / "outputs" / "logs" / "signal_audit_log.csv"
VENDOR_MASTER_FILE = BASE_DIR / "data" / "vendor_master.xlsx"


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


@st.cache_data
def load_vendor_master_suppliers() -> pd.DataFrame:
    if not VENDOR_MASTER_FILE.exists():
        return pd.DataFrame()

    try:
        workbook = pd.read_excel(VENDOR_MASTER_FILE, sheet_name=None)
        suppliers = workbook.get("suppliers", pd.DataFrame()).copy()
        categories = workbook.get("categories", pd.DataFrame()).copy()

        if not suppliers.empty and not categories.empty:
            suppliers = suppliers.merge(
                categories[["category_id", "category_name"]],
                on="category_id",
                how="left",
            )

        return suppliers
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


def build_portfolio_summary(all_suppliers_df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if all_suppliers_df.empty:
        return summary_df.copy()

    all_suppliers_df = all_suppliers_df.copy()

    if summary_df.empty:
        merged = all_suppliers_df.copy()
        merged["total_risk_score"] = 0
        merged["max_possible_score"] = 27
        merged["event_count"] = 0
        merged["high_severity_events"] = 0
        merged["priority_level"] = "Ignore"
        merged["risk_reason"] = "No material public risk signals detected today."
        return merged

    summary_df = summary_df.copy()

    # keep only one row per supplier in case of duplicates
    summary_df = summary_df.drop_duplicates(subset=["supplier_id"])

    merged = all_suppliers_df.merge(
        summary_df,
        on=["supplier_id", "supplier_name", "category_id", "category_name"],
        how="left",
    )

    merged["total_risk_score"] = merged["total_risk_score"].fillna(0)
    merged["max_possible_score"] = merged["max_possible_score"].fillna(27)
    merged["event_count"] = merged["event_count"].fillna(0)
    merged["high_severity_events"] = merged["high_severity_events"].fillna(0)
    merged["priority_level"] = merged["priority_level"].fillna("Ignore")
    merged["risk_reason"] = merged["risk_reason"].fillna("No material public risk signals detected today.")

    for col in ["total_risk_score", "max_possible_score", "event_count", "high_severity_events"]:
        merged[col] = merged[col].astype(int)

    return merged


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

    filtered_signals = signals_df[signals_df["supplier_name"].isin(filtered_supplier_names)] if not signals_df.empty and filtered_supplier_names else signals_df
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

    st.markdown("### 4. Severity × Confidence Reference Matrix")
    matrix_df = pd.DataFrame({
        "Severity \\ Confidence": ["Low", "Medium", "High"],
        "Low": [1, 2, 3],
        "Medium": [2, 4, 6],
        "High": [3, 6, 9],
    })
    st.dataframe(matrix_df, use_container_width=True, hide_index=True)


def build_lollipop_chart(category_df: pd.DataFrame, category_name: str) -> go.Figure:
    plot_df = category_df.copy()

    if plot_df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"{category_name} (0 vendors)",
            height=300,
            xaxis_title="Risk Score",
            yaxis_title="Supplier",
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        return fig

    plot_df["total_risk_score"] = pd.to_numeric(plot_df["total_risk_score"], errors="coerce").fillna(0)
    plot_df["max_possible_score"] = pd.to_numeric(plot_df["max_possible_score"], errors="coerce").fillna(27)
    plot_df["event_count"] = pd.to_numeric(plot_df["event_count"], errors="coerce").fillna(0)

    plot_df = plot_df.sort_values(
        by=["total_risk_score", "event_count", "supplier_name"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    max_possible = int(max(27, plot_df["max_possible_score"].max()))
    axis_padding = 4
    axis_max = max_possible + axis_padding

    status_color_map = {
        "Ignore": "#A7B0BE",
        "Watch": "#F2C94C",
        "Alert": "#F2994A",
        "Escalate": "#EB5757",
    }
    zero_box_color = "#DCE6F2"
    zero_line_color = "#C7D3E3"

    fig = go.Figure()

    # Background score bands to make higher scores easier to interpret visually.
    risk_bands = [
        (0, 5, "rgba(167,176,190,0.10)"),
        (5, 10, "rgba(242,201,76,0.12)"),
        (10, 18, "rgba(242,153,74,0.12)"),
        (18, max_possible, "rgba(235,87,87,0.10)"),
    ]
    for x0, x1, fillcolor in risk_bands:
        fig.add_vrect(x0=x0, x1=x1, fillcolor=fillcolor, line_width=0, layer="below")

    suppliers = plot_df["supplier_name"].tolist()

    # Full baseline track to max possible score.
    for supplier in suppliers:
        fig.add_trace(
            go.Scatter(
                x=[0, max_possible],
                y=[supplier, supplier],
                mode="lines",
                line=dict(color="#E8EDF3", width=8),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    # Active score segment.
    for _, row in plot_df.iterrows():
        score = float(row["total_risk_score"])
        supplier = row["supplier_name"]
        segment_color = zero_line_color if score == 0 else status_color_map.get(row["priority_level"], "#A7B0BE")
        fig.add_trace(
            go.Scatter(
                x=[0, score],
                y=[supplier, supplier],
                mode="lines",
                line=dict(color=segment_color, width=8),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    marker_colors = [
        zero_box_color if score == 0 else status_color_map.get(priority, "#A7B0BE")
        for score, priority in zip(plot_df["total_risk_score"], plot_df["priority_level"])
    ]
    marker_symbols = ["square" if score == 0 else "circle" for score in plot_df["total_risk_score"]]
    marker_sizes = [15 if score == 0 else 17 for score in plot_df["total_risk_score"]]

    fig.add_trace(
        go.Scatter(
            x=plot_df["total_risk_score"],
            y=suppliers,
            mode="markers",
            marker=dict(
                color=marker_colors,
                size=marker_sizes,
                symbol=marker_symbols,
                line=dict(color="#4B5563", width=1.2),
            ),
            customdata=plot_df[["priority_level", "event_count", "risk_reason", "max_possible_score", "category_name"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Category: %{customdata[4]}<br>"
                "Risk Score: %{x}/%{customdata[3]}<br>"
                "Status: %{customdata[0]}<br>"
                "Signals: %{customdata[1]}<br>"
                "Why: %{customdata[2]}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    # Score badges at the right so higher-risk suppliers do not get visually cramped.
    badge_x = max_possible + 1.2
    for _, row in plot_df.iterrows():
        score = int(row["total_risk_score"])
        badge_color = zero_box_color if score == 0 else status_color_map.get(row["priority_level"], "#A7B0BE")
        fig.add_annotation(
            x=badge_x,
            y=row["supplier_name"],
            text=f"{score}/{int(row['max_possible_score'])}",
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            font=dict(size=11, color="#1F2937"),
            bgcolor=badge_color,
            bordercolor="#CBD5E1",
            borderwidth=1,
            borderpad=4,
            opacity=0.98,
        )

    fig.update_layout(
        title=f"{category_name} ({len(plot_df)} vendors)",
        height=max(360, 72 + 52 * len(plot_df)),
        margin=dict(l=10, r=90, t=52, b=34),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            title="Risk Score",
            range=[0, axis_max],
            showgrid=True,
            gridcolor="#E5E7EB",
            zeroline=False,
            tickmode="array",
            tickvals=list(range(0, max_possible + 1, 3)) if max_possible > 12 else list(range(0, max_possible + 1)),
        ),
        yaxis=dict(
            title="",
            categoryorder="array",
            categoryarray=suppliers,
            autorange="reversed",
            showgrid=False,
            tickfont=dict(size=12),
        ),
    )

    # Reference separators between score bands.
    for x in [5, 10, 18]:
        if x < axis_max:
            fig.add_vline(x=x, line_width=1, line_dash="dot", line_color="#94A3B8")

    return fig


def render_supplier_risk_lollipop_section(filtered_summary: pd.DataFrame):
    st.markdown("### Supplier Risk by Category")
    st.caption(
        "One lollipop chart is shown for each vendor category. Vendors stay only in their assigned category. "
        "Zero-risk suppliers remain visible with a separate square marker and score badge."
    )

    if filtered_summary.empty:
        st.info("No supplier data available.")
        return

    chart_df = filtered_summary.copy()
    chart_df = chart_df.dropna(subset=["supplier_name", "category_name"])

    if chart_df.empty:
        st.info("No category data available.")
        return

    category_order = (
        chart_df.groupby("category_name", dropna=False)["total_risk_score"]
        .max()
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    for category_name in category_order:
        category_df = chart_df[chart_df["category_name"] == category_name].copy()
        category_df = category_df.sort_values(
            by=["total_risk_score", "event_count", "supplier_name"],
            ascending=[False, False, True],
        )

        fig = build_lollipop_chart(category_df, category_name)
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=f"lollipop_{category_name}",
        )

    legend_cols = st.columns(5)
    legend_cols[0].markdown("**Legend**")
    legend_cols[1].markdown("⬜ 0 Risk")
    legend_cols[2].markdown("🟡 Watch")
    legend_cols[3].markdown("🟠 Alert")
    legend_cols[4].markdown("🔴 Escalate")

def supplier_breakdown_section(
    selected_supplier: str,
    summary_df: pd.DataFrame,
    signals_df: pd.DataFrame,
    chart_key_suffix: str = "default"
):
    st.markdown("### Risk Composition by Signal Type")

    if signals_df.empty:
        st.info("No signals available.")
        return

    vendor_options = ["All Vendors"] + sorted(signals_df["supplier_name"].dropna().unique().tolist())

    default_vendor = selected_supplier if selected_supplier in vendor_options else "All Vendors"

    chosen_vendor = st.selectbox(
        "Choose vendor for risk composition",
        vendor_options,
        index=vendor_options.index(default_vendor) if default_vendor in vendor_options else 0,
        key=f"vendor_composition_select_{chart_key_suffix}"
    )

    if chosen_vendor == "All Vendors":
        chart_signals = signals_df.copy()
    else:
        chart_signals = signals_df[signals_df["supplier_name"] == chosen_vendor].copy()

    comp_df = (
        chart_signals.groupby("signal_type", as_index=False)
        .agg(total_score=("base_risk_score", "sum"))
        .sort_values("total_score", ascending=False)
    )

    if not comp_df.empty:
        fig = px.bar(
            comp_df,
            x="signal_type",
            y="total_score",
            color="signal_type",
            title=f"Risk Composition - {chosen_vendor}",
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=f"risk_composition_chart_{chart_key_suffix}_{chosen_vendor}"
        )
    else:
        st.info("No signal composition data available.")

    if chosen_vendor != "All Vendors":
        supplier_row = summary_df[summary_df["supplier_name"] == chosen_vendor].head(1)

        if not supplier_row.empty:
            row = supplier_row.iloc[0]

            st.markdown("### Supplier Score Summary")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Raw Score", f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])}")
            c2.metric("Status", row["priority_level"])
            c3.metric("Signals", int(row["event_count"]))
            c4.metric("High Severity Events", int(row["high_severity_events"]))

            st.markdown("**Why this score?**")
            st.write(row["risk_reason"])
            st.info(get_score_interpretation(row["total_risk_score"]))

        supplier_signals = signals_df[signals_df["supplier_name"] == chosen_vendor].copy()

        if not supplier_signals.empty:
            st.markdown("### Top Signals Driving Risk")

            top_signals = supplier_signals.sort_values(
                "base_risk_score", ascending=False
            ).head(3)

            for _, row in top_signals.iterrows():
                st.markdown(
                    f"""
**{row['signal_type'].upper()}**
- Severity: {row['severity']}
- Confidence: {row['confidence']}
- Event Score: {int(row['base_risk_score'])}
- Why it matters: {row['llm_reason']}
- Recommended action: {row['recommended_action']}
"""
                )

            st.markdown("### Detailed Signal Breakdown")

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


def main():
    summary_df, signals_df, history_df, audit_df = load_data()
    all_suppliers_df = load_vendor_master_suppliers()

    st.title("📡 Supplier Watchtower")
    today_str = datetime.now().strftime("%B %d, %Y")
    st.markdown(f"**Today's Date:** {today_str}")
    st.markdown("Welcome to our AI Agent for Supplier Evaluation and Risk Monitoring.")

    if not history_df.empty and "run_date" in history_df.columns:
        last_run = pd.to_datetime(history_df["run_date"], errors="coerce").max()
        if pd.notna(last_run):
            st.markdown(f"**Last Run Date:** {last_run.strftime('%B %d, %Y')}")

    # Build executive-level summary for all suppliers
    summary_df = build_portfolio_summary(all_suppliers_df, summary_df)

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
        "Risk Methodology",
        "Supplier Drilldown",
        "Signals & Evidence",
        "Audit & History",
    ])

    with tabs[0]:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Vendors Monitored", len(filtered_summary))
        col2.metric(
            "Vendors with Active Signals",
            int((filtered_summary["event_count"] > 0).sum()) if not filtered_summary.empty else 0,
        )
        col3.metric(
            "Alerts / Escalations",
            int(filtered_summary["priority_level"].isin(["Alert", "Escalate"]).sum()) if not filtered_summary.empty else 0,
        )
        col4.metric(
            "Healthy / No Material Risk",
            int((filtered_summary["total_risk_score"] == 0).sum()) if not filtered_summary.empty else 0,
        )

        st.markdown("### Top Risks Today")

        risky_suppliers = filtered_summary[filtered_summary["total_risk_score"] > 0].copy()

        if not risky_suppliers.empty:
            top_risks = risky_suppliers.sort_values(
                by=["total_risk_score", "event_count"],
                ascending=[False, False]
            ).head(3)

            for _, row in top_risks.iterrows():
                st.error(
                    f"{row['supplier_name']} | {row['priority_level']} | "
                    f"{int(row['total_risk_score'])}/{int(row['max_possible_score'])} "
                    f"— {row['risk_reason']}"
                )
        else:
            st.success("No material supplier risk signals detected for the current filter.")

        st.divider()

        left, right = st.columns([2.2, 1])

        with left:
            render_supplier_risk_lollipop_section(filtered_summary)

        with right:
            st.markdown("### Highest Priority Suppliers")
            if not filtered_summary.empty:
                priority_view = filtered_summary[
                    filtered_summary["total_risk_score"] > 0
                ].sort_values(
                    by=["total_risk_score", "event_count"],
                    ascending=[False, False]
                ).head(5)

                if not priority_view.empty:
                    priority_cols = [
                        "supplier_name",
                        "category_name",
                        "Score",
                        "priority_level",
                    ]
                    priority_view = priority_view[priority_cols].rename(columns={
                        "supplier_name": "Supplier",
                        "category_name": "Category",
                        "priority_level": "Status",
                    })

                    st.dataframe(priority_view, use_container_width=True, hide_index=True)
                else:
                    st.success("No suppliers with active risk signals right now.")
            else:
                st.info("No supplier priority data available.")

        st.divider()

        st.markdown("### Supplier Portfolio Overview")
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
        methodology_section()

        st.divider()

        st.markdown("### How to Read the Score")

        st.markdown("""
A supplier’s score is meant to answer one question:

**How much credible risk signal are we seeing for this supplier right now?**

#### Step 1 — Each signal gets an event score
Each signal is scored using:

**Event Score = Severity × Confidence**

- **Severity** tells us how serious the issue could be.
- **Confidence** tells us how much we trust the signal and its source.

#### Step 2 — Supplier score is the sum of event scores
A supplier’s total score is the sum of all relevant event scores detected in the current run.

#### How to interpret the result
- **0** → No material public risk signals detected
- **1–5** → Minor signal; log and continue monitoring
- **6–10** → Watch; moderate signal that merits review
- **11–18** → Alert; meaningful risk, likely needs action
- **19+** → Escalate; multiple strong signals or a highly credible major issue

#### What makes a score go up?
A score increases when:
- more than one signal is detected
- signals are high severity
- signals come from more credible sources
- multiple signals reinforce the same risk theme

#### Important note
A higher score does **not** just mean “more bad news.”  
It means the system found **more credible and more material evidence** that the supplier may require attention.
""")

        st.info(
            "Use the Supplier Drilldown tab to see exactly which signals contributed to the score, "
            "what type of risk they represent, and what action is recommended."
        )

    with tabs[2]:
        if filtered_supplier_names:
            supplier_breakdown_section(
                selected_supplier="All Vendors",
                summary_df=filtered_summary,
                signals_df=filtered_signals,
                chart_key_suffix="drilldown"
            )
        else:
            st.info("No suppliers available for drilldown.")

    with tabs[3]:
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
