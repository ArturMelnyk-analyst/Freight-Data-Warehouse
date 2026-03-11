from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def run_step(module_name: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"RUNNING: python -m {module_name}")
    print(f"{'=' * 70}\n")

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=ROOT,
        check=False
    )

    if result.returncode != 0:
        raise RuntimeError(f"Step failed: {module_name}")

    print(f"\nCompleted: {module_name}\n")


def main() -> None:
    """
    Run the full warehouse pipeline in the correct order.

    Order:
    1) dimensions
    2) staging
    3) fact build
    4) validation
    """
    steps = [
        "etl.02_build_dimensions",
        "etl.03_load_staging",
        "etl.04_build_fact",
        "etl.05_validate",
    ]

    for step in steps:
        run_step(step)

    print("\nAll ETL pipeline steps completed successfully.\n")


if __name__ == "__main__":
    main()