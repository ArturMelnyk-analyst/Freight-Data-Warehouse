# etl/run_log.py
import uuid
from datetime import datetime
from sqlalchemy import text

def start_run(engine, phase: str = None, process_name: str = None) -> str:
    """
    Create a new row in etl_run_log.
    We standardize on 'phase' (matches DB column).
    process_name is accepted as an alias to avoid future mismatch errors.
    """
    if phase is None:
        phase = process_name  # alias support
    if not phase:
        raise ValueError("start_run requires 'phase' (or process_name as alias).")

    run_id = str(uuid.uuid4())

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_run_log (run_id, phase, status, start_ts)
                VALUES (:run_id, :phase, :status, :start_ts)
            """),
            {
                "run_id": run_id,
                "phase": phase,
                "status": "RUNNING",
                "start_ts": datetime.now(),
            }
        )
    return run_id


def end_run(
    engine,
    run_id: str,
    status: str,
    notes: str = None,
    source_file: str = None,
    file_hash: str = None,
    file_modified_ts=None,
    stg_row_count: int = None,
):
    """
    Finalize a run row in etl_run_log.
    Only writes columns that exist in your schema (you already added these in PR#3).
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_run_log
                SET status = :status,
                    end_ts = :end_ts,
                    notes = :notes,
                    source_file = :source_file,
                    file_hash = :file_hash,
                    file_modified_ts = :file_modified_ts,
                    stg_row_count = :stg_row_count
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "status": status,
                "end_ts": datetime.now(),
                "notes": notes,
                "source_file": source_file,
                "file_hash": file_hash,
                "file_modified_ts": file_modified_ts,
                "stg_row_count": stg_row_count,
            }
        )
