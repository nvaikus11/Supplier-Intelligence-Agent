from pathlib import Path
import subprocess
import sys
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent.parent

FILES_TO_PUSH = [
    "outputs/daily_reports/supplier_risk_summary.csv",
    "outputs/daily_reports/scored_signals.csv",
    "outputs/history/supplier_risk_history.csv",
    "outputs/logs/signal_audit_log.csv",
    "outputs/daily_reports/daily_brief.txt",
]


def run_git_command(args):
    result = subprocess.run(
        ["git"] + args,
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
    )
    return result


def main():
    # Check repo
    result = run_git_command(["rev-parse", "--is-inside-work-tree"])
    if result.returncode != 0:
        print("Not inside a git repository. Skipping GitHub publish.")
        sys.exit(0)

    # Check remote
    remote_result = run_git_command(["remote", "-v"])
    if remote_result.returncode != 0 or not remote_result.stdout.strip():
        print("No git remote configured. Skipping GitHub publish.")
        sys.exit(0)

    # Add files
    add_result = run_git_command(["add", "-f"] + FILES_TO_PUSH)
    if add_result.returncode != 0:
        print("Git add failed.")
        print(add_result.stderr)
        sys.exit(add_result.returncode)

    # Check if there is anything to commit
    status_result = run_git_command(["status", "--porcelain"])
    if status_result.returncode != 0:
        print("Git status failed.")
        print(status_result.stderr)
        sys.exit(status_result.returncode)

    if not status_result.stdout.strip():
        print("No output changes to commit.")
        sys.exit(0)

    commit_message = f"Update watchtower outputs - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    commit_result = run_git_command(["commit", "-m", commit_message])
    if commit_result.returncode != 0:
        print("Git commit failed.")
        print(commit_result.stderr)
        sys.exit(commit_result.returncode)

    push_result = run_git_command(["push"])
    if push_result.returncode != 0:
        print("Git push failed.")
        print(push_result.stderr)
        sys.exit(push_result.returncode)

    print("Outputs pushed to GitHub successfully.")


if __name__ == "__main__":
    main()