from pathlib import Path
import json
import subprocess
import sys
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATE_PAGES_FILE = BASE_DIR / "outputs" / "daily_reports" / "browser_candidate_pages.csv"


def run_script(script: str) -> None:
    print(f"\nRunning {script}...")
    result = subprocess.run([sys.executable, script], cwd=BASE_DIR)

    if result.returncode != 0:
        raise RuntimeError(f"{script} failed with exit code {result.returncode}")


def close_browser_tabs_from_candidates() -> None:
    if not CANDIDATE_PAGES_FILE.exists():
        print("\nNo browser candidate pages file found. Skipping browser cleanup.\n")
        return

    candidates_df = pd.read_csv(CANDIDATE_PAGES_FILE)
    if candidates_df.empty or "page_url" not in candidates_df.columns:
        print("\nNo candidate page URLs found. Skipping browser cleanup.\n")
        return

    candidate_urls = set(candidates_df["page_url"].dropna().astype(str).tolist())

    tabs_cmd = [
        "openclaw",
        "browser",
        "--browser-profile",
        "openclaw",
        "--json",
        "tabs",
    ]

    tabs_result = subprocess.run(tabs_cmd, capture_output=True, text=True, cwd=BASE_DIR)

    if tabs_result.returncode != 0:
        print("\nCould not list browser tabs. Skipping cleanup.\n")
        print(tabs_result.stderr)
        return

    try:
        tabs_payload = json.loads(tabs_result.stdout)
    except json.JSONDecodeError:
        print("\nCould not parse browser tabs output. Skipping cleanup.\n")
        print(tabs_result.stdout)
        return

    tabs = tabs_payload if isinstance(tabs_payload, list) else tabs_payload.get("tabs", [])

    closed = 0
    for tab in tabs:
        target_id = str(tab.get("targetId") or tab.get("id") or "").strip()
        url = str(tab.get("url") or "").strip()

        if target_id and url in candidate_urls:
            close_cmd = [
                "openclaw",
                "browser",
                "--browser-profile",
                "openclaw",
                "close",
                target_id,
            ]
            close_result = subprocess.run(close_cmd, capture_output=True, text=True, cwd=BASE_DIR)
            if close_result.returncode == 0:
                closed += 1

    print(f"\nClosed {closed} OpenClaw browser tab(s).\n")


if __name__ == "__main__":
    run_script("src/browser_research.py")
    run_script("src/browser_enrich_and_score.py")
    run_script("src/risk_scoring.py")
    run_script("src/generate_daily_brief.py")
    run_script("src/send_watchtower_alert.py")

    # close_browser_tabs_from_candidates()

    print("\nSupplier Watchtower browser-first pipeline completed.\n")