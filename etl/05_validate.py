from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.utils.db import get_engine


DB = "faf_dw"
RUN_LOG_TABLE = "etl_run_log"
CHECK_LOG_TABLE = "etl_check_log"

VALIDATION_DIR = Path("sql") / "validation"

# Hard-fail checks (gates)
HARD_FAIL_CHECKS = {
    "01_expected_rowcount.sql",
    "02_orphan_checks.sql",
    "03_duplicate_grain.sql",
    "04_negative_measures.sql",
    "05_null_keys.sql",
}

# Soft check (warn only unless threshold exceeded)
SOFT_CHECKS = {
    "06_zone_complementarity.sql",
}

# Thresholds (tune later)
ZONE_COMPLEMENTARITY_FAIL_IF_GT = 0.005  # 0.5% of staging rows


@dataclass
class CheckResult:
    check_file: str
    check_name: str
    failed_rows: int
    status: str  # PASS / FAIL / WARN
    details: Optional[str] = None


def start_run(engine: Engine, phase: str) -> str:
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {RUN_LOG_TABLE} (run_id, phase, status, start_ts)
                VALUES (:run_id, :phase, 'RUNNING', :start_ts)
            """),
            {"run_id": run_id, "phase": phase, "start_ts": datetime.now()},
        )
    return run_id


def end_run(engine: Engine, run_id: str, status: str, notes: Optional[str] = None) -> None:
    if notes and len(notes) > 2000:
        notes = notes[:2000] + " ...[truncated]"
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE {RUN_LOG_TABLE}
                SET status = :status,
                    end_ts = :end_ts,
                    notes = :notes
                WHERE run_id = :run_id
            """),
            {"run_id": run_id, "status": status, "end_ts": datetime.now(), "notes": notes},
        )


def log_check(engine: Engine, run_id: str, r: CheckResult) -> None:
    allowed = {"PASS", "FAIL", "INFO", "ERROR"}
    status = r.status if r.status in allowed else "ERROR"

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {CHECK_LOG_TABLE}
                    (run_id, check_name, failed_rows, status, created_at)
                VALUES
                    (:run_id, :check_name, :failed_rows, :status, :created_at)
            """),
            {
                "run_id": run_id,
                "check_name": f"{r.check_file}::{r.check_name}",
                "failed_rows": int(r.failed_rows),
                "status": status,
                "created_at": datetime.now(),
            },
        )



def run_sql_file(engine: Engine, sql_path: Path) -> pd.DataFrame:
    """
    Runs a .sql file that may contain:
      - USE faf_dw;
      - multiple SELECT statements
      - non-SELECT statements
    Returns a single dataframe that is the concatenation of ALL SELECT outputs.
    """
    sql = sql_path.read_text(encoding="utf-8")

    # Split on semicolons (simple, ok for your validation files)
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    dfs: List[pd.DataFrame] = []

    with engine.connect() as conn:
        # Avoid endless queries (server-side timeout; ms)
        conn.execute(text("SET SESSION max_execution_time = 60000"))  # 60s per statement

        for stmt in statements:
            low = stmt.lower().strip()

            # handle USE
            if low.startswith("use "):
                conn.execute(text(stmt))
                continue

            # SELECT => capture output
            if low.startswith("select"):
                df = pd.read_sql(text(stmt), conn)
                if not df.empty:
                    dfs.append(df)
                else:
                    # empty df is still a valid result — keep it (helps debugging)
                    dfs.append(df)
            else:
                # DDL/DML (should be rare in validation)
                conn.execute(text(stmt))

    if not dfs:
        return pd.DataFrame()

    # Concatenate all SELECT outputs vertically
    # (If they have different columns, pandas will align columns and fill NaN)
    out = pd.concat(dfs, ignore_index=True, sort=False)
    return out


def _stem_name(sql_file: str) -> str:
    # "03_duplicate_grain.sql" -> "duplicate_grain"
    name = sql_file
    if name.endswith(".sql"):
        name = name[:-4]
    # drop leading numeric prefix if present
    parts = name.split("_", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return parts[1]
    return name


def interpret_result(check_file: str, df: pd.DataFrame, engine: Engine) -> List[CheckResult]:
    """
    Accept flexible output shapes and normalize to:
      check_name, failed_rows

    Supported outputs:
      A) check_name + failed_rows
      B) check_name + orphan_id_count / failed_ids
      C) only failed_rows (then check_name = <file stem>)
      D) check_name + has_orphans / has_duplicates (0/1)
    """
    results: List[CheckResult] = []

    if df is None or df.empty:
        results.append(CheckResult(check_file, "no_result_returned", 1, "FAIL", "Validation query returned no rows."))
        return results

    # Lowercase mapping
    cols = {c.lower(): c for c in df.columns}

    check_col = cols.get("check_name")

    # main numeric outputs we accept
    failed_col = (
        cols.get("failed_rows")
        or cols.get("orphan_id_count")
        or cols.get("failed_ids")
        or cols.get("has_orphans")
        or cols.get("has_duplicates")
        or cols.get("has_dupes")
    )

    # Case C: only one numeric column (no check_name)
    if check_col is None:
        # If dataframe has exactly 1 column, treat it as failed metric
        if len(df.columns) == 1:
            only_col = df.columns[0]
            try:
                failed = int(df.iloc[0][only_col])
            except Exception:
                results.append(
                    CheckResult(check_file, "unexpected_columns", 1, "FAIL", f"Columns: {list(df.columns)}")
                )
                return results

            results.append(CheckResult(check_file, _stem_name(check_file), failed, "PASS" if failed == 0 else "FAIL"))
        else:
            # no check_name + multiple columns => we need a known failed column
            if failed_col is None:
                results.append(
                    CheckResult(check_file, "unexpected_columns", 1, "FAIL", f"Columns: {list(df.columns)}")
                )
                return results

            # Build synthetic check_name per row
            for i, row in df.iterrows():
                try:
                    failed = int(row[failed_col])
                except Exception:
                    failed = 1
                name = f"{_stem_name(check_file)}_row{i+1}"
                results.append(CheckResult(check_file, name, failed, "PASS" if failed == 0 else "FAIL"))

        return _post_process_soft_checks(check_file, results, engine)

    # Case A/B/D: check_name exists but failed_col missing
    if failed_col is None:
        results.append(
            CheckResult(check_file, "unexpected_columns", 1, "FAIL", f"Columns: {list(df.columns)}")
        )
        return results

    # Normal row-by-row conversion
    for _, row in df.iterrows():
        check_name = str(row[check_col])
        try:
            failed = int(row[failed_col])
        except Exception:
            failed = 1

        status = "PASS" if failed == 0 else "FAIL"
        results.append(CheckResult(check_file, check_name, failed, status))

    return _post_process_soft_checks(check_file, results, engine)


def _post_process_soft_checks(check_file: str, results: List[CheckResult], engine: Engine) -> List[CheckResult]:
    """
    Soft-check logic only for zone complementarity.
    Soft checks should not fail the pipeline: they produce INFO when non-zero.
    """
    if check_file not in SOFT_CHECKS:
        return results

    stg_cnt = int(pd.read_sql(text("SELECT COUNT(*) AS c FROM stg_faf"), engine).loc[0, "c"] or 0)

    for r in results:
        rate = (r.failed_rows / stg_cnt) if stg_cnt else 0

        if r.failed_rows == 0:
            r.status = "PASS"
            r.details = None
        else:
            # match DB enum: use INFO (not WARN)
            r.status = "INFO"
            r.details = f"Rate {rate:.4%} (informational)"

    return results




def main() -> None:
    engine = get_engine()

    sql_files = sorted([p for p in VALIDATION_DIR.glob("*.sql") if p.is_file()])
    if not sql_files:
        raise RuntimeError(f"No validation SQL files found in {VALIDATION_DIR}")

    run_id = start_run(engine, phase="validation")

    hard_fail = False
    soft_issues: List[str] = []   # WARN/FAIL from SOFT checks
    hard_notes: List[str] = []    # FAIL from HARD checks

    try:
        print("Running data quality gates...")
        for p in sql_files:
            print(f" - {p.as_posix()}")

            df = run_sql_file(engine, p)
            results = interpret_result(p.name, df, engine)

            for r in results:
                log_check(engine, run_id, r)

                # HARD gates: FAIL => stop pipeline
                if p.name in HARD_FAIL_CHECKS and r.status == "FAIL":
                    hard_fail = True
                    hard_notes.append(f"{r.check_file}::{r.check_name} failed_rows={r.failed_rows}")

                # SOFT checks: collect WARN/FAIL for summary (do NOT stop pipeline)
                if p.name in SOFT_CHECKS and r.status in ("WARN", "FAIL"):
                    extra = f" ({r.details})" if r.details else ""
                    soft_issues.append(f"{r.check_file}::{r.check_name} -> {r.failed_rows}{extra}")

                extra = f" ({r.details})" if r.details else ""
                print(f"   {r.status}: {r.check_name} -> {r.failed_rows}{extra}")

        # Decide final outcome
        if hard_fail:
            end_run(engine, run_id, status="FAILED", notes=" | ".join(hard_notes))
            raise RuntimeError("Validation failed (hard gates). See etl_check_log for details.")

        # Hard gates passed, but soft issues exist
        if soft_issues:
            note = "Hard gates passed; soft checks need review: " + " | ".join(soft_issues)
            end_run(engine, run_id, status="SUCCESS", notes=note)
            print("Hard gates PASSED ✅  (pipeline can continue)")
            print("Soft checks need review ⚠️")
            for s in soft_issues:
                print(f" - {s}")
            return

        # Everything clean
        end_run(engine, run_id, status="SUCCESS", notes="All validation checks passed.")
        print("All validation checks passed ✅")

    except Exception as e:
        try:
            end_run(engine, run_id, status="FAILED", notes=str(e))
        except Exception:
            pass
        raise



if __name__ == "__main__":
    main()

