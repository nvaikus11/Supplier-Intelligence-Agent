from pathlib import Path
import subprocess
import sys


BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"


def run_script(script_name: str) -> None:
    script_path = SRC_DIR / script_name
    print(f"\nRunning {script_name} ...")
    result = subprocess.run([sys.executable, str(script_path)], cwd=BASE_DIR)

    if result.returncode != 0:
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")


if __name__ == "__main__":
    run_script("collect_signals.py")
    run_script("risk_scoring.py")
    run_script("generate_daily_brief.py")

    print("\nSupplier Watchtower pipeline completed successfully.\n")