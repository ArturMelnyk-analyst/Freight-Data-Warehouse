import uuid
from datetime import datetime
from sqlalchemy import text

def start_run(engine, phase: str) -> str:
    run_id = str(uuid.uuid4())
    now = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_run_log (run_id, phase, status, start_ts)
            VALUES (:run_id, :phase, 'STARTED', :start_ts)
        """), {"run_id": run_id, "phase": phase, "start_ts": now})

    return run_id

def end_run_success(engine, run_id: str, counts: dict):
    now = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_run_log
            SET status='SUCCESS', end_ts=:end_ts,
                dim_zone_count=:dim_zone_count,
                dim_mode_count=:dim_mode_count,
                dim_commodity_count=:dim_commodity_count,
                dim_trade_type_count=:dim_trade_type_count,
                dim_distance_band_count=:dim_distance_band_count,
                dim_year_count=:dim_year_count
            WHERE run_id=:run_id
        """), {"run_id": run_id, "end_ts": now, **counts})

def end_run_failed(engine, run_id: str, error_msg: str):
    now = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_run_log
            SET status='FAILED', end_ts=:end_ts, notes=:notes
            WHERE run_id=:run_id
        """), {"run_id": run_id, "end_ts": now, "notes": error_msg})