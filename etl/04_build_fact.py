"""
PR#4 — Build fact table (wide -> long) from staging, memory-safe.

Source: MySQL staging table stg_faf (loaded in PR#3)
Target: fact_faf (created in PR#4)

Approach:
- Chunk staging by stg_id ranges.
- Map codes -> dimension IDs via preloaded lookup dicts.
- Expand each staging row into up to 7 long rows (2018-2024),
  one per year, carrying 4 measures: tons, value, current_value, tmiles.
- Drop year rows where all measures are null.
- Aggregate duplicates inside the chunk.
- CRITICAL: Insert with UPSERT (ON DUPLICATE KEY UPDATE) to handle duplicates ACROSS chunks.
- Write expected vs actual counts into etl_run_log.

Run:
  python -m etl.04_build_fact
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import math
import uuid

import pandas as pd
from sqlalchemy import text, Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import func

from etl.utils.db import get_engine


# ----------------------------
# Settings
# ----------------------------
STG_TABLE = "stg_faf"
FACT_TABLE = "fact_faf"
RUN_LOG_TABLE = "etl_run_log"

YEARS = list(range(2018, 2025))  # 2018..2024

STG_ID_CHUNK = 100_000           # staging pull by stg_id range
UPSERT_BATCH = 5_000             # executemany size for UPSERT (safe + stable)

TRUNCATE_FACT_AT_START = True    # recommended until pipeline is stable


# ----------------------------
# Helpers
# ----------------------------
def norm_code(x) -> Optional[str]:
    """Normalize categorical codes from staging."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan" or s == "0":
        return None
    if s.endswith(".0"):
        s = s[:-2]
    return s


def make_zone_key(dms_code: Optional[str], fr_code: Optional[str]) -> Optional[str]:
    """Unified zone key used in dim_zone: DMS-<code> or FR-<code>."""
    if dms_code:
        return f"DMS-{dms_code}"
    if fr_code:
        return f"FR-{fr_code}"
    return None


def safe_note(msg: str, max_len: int = 1500) -> str:
    """Prevent etl_run_log.notes from exploding on huge SQL errors."""
    if msg is None:
        return None
    s = str(msg)
    return s if len(s) <= max_len else s[:max_len] + " ...[truncated]"


@dataclass
class DimLookups:
    zone: Dict[str, int]
    mode: Dict[str, int]
    commodity: Dict[str, int]
    trade_type: Dict[str, int]
    dist_band: Dict[str, int]


def load_dim_lookups(engine: Engine) -> DimLookups:
    """Load dimensions into dicts for fast mapping."""
    # dim_zone
    z = pd.read_sql(text("SELECT zone_id, zone_type, zone_code FROM dim_zone"), engine)
    z["zone_type"] = z["zone_type"].astype(str).str.strip()
    z["zone_code"] = z["zone_code"].astype(str).str.strip().apply(norm_code)
    z["zone_key"] = z["zone_type"] + "-" + z["zone_code"].astype(str)
    zone = dict(zip(z["zone_key"], z["zone_id"]))

    # dim_mode
    m = pd.read_sql(text("SELECT mode_id, mode_code FROM dim_mode"), engine)
    m["mode_code"] = m["mode_code"].apply(norm_code)
    mode = dict(zip(m["mode_code"], m["mode_id"]))

    # dim_commodity
    c = pd.read_sql(text("SELECT commodity_id, sctg2 FROM dim_commodity"), engine)
    c["sctg2"] = c["sctg2"].apply(norm_code)
    commodity = dict(zip(c["sctg2"], c["commodity_id"]))

    # dim_trade_type
    t = pd.read_sql(text("SELECT trade_type_id, trade_type_code FROM dim_trade_type"), engine)
    t["trade_type_code"] = t["trade_type_code"].apply(norm_code)
    trade_type = dict(zip(t["trade_type_code"], t["trade_type_id"]))

    # dim_distance_band
    d = pd.read_sql(text("SELECT dist_band_id, dist_band_code FROM dim_distance_band"), engine)
    d["dist_band_code"] = d["dist_band_code"].apply(norm_code)
    dist_band = dict(zip(d["dist_band_code"], d["dist_band_id"]))

    return DimLookups(zone=zone, mode=mode, commodity=commodity, trade_type=trade_type, dist_band=dist_band)


def start_run_row(engine: Engine, phase: str) -> str:
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


def end_run_row(
    engine: Engine,
    run_id: str,
    status: str,
    notes: Optional[str] = None,
    expected_fact_rows: Optional[int] = None,
    fact_row_count: Optional[int] = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE {RUN_LOG_TABLE}
                SET status = :status,
                    end_ts = :end_ts,
                    notes = :notes,
                    expected_fact_rows = :expected_fact_rows,
                    fact_row_count = :fact_row_count
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "status": status,
                "end_ts": datetime.now(),
                "notes": safe_note(notes) if notes else None,
                "expected_fact_rows": expected_fact_rows,
                "fact_row_count": fact_row_count,
            },
        )


def get_staging_bounds(engine: Engine) -> Tuple[int, int]:
    df = pd.read_sql(text(f"SELECT COUNT(*) AS cnt, MAX(stg_id) AS mx FROM {STG_TABLE}"), engine)
    return int(df.loc[0, "cnt"] or 0), int(df.loc[0, "mx"] or 0)


def fetch_staging_chunk(engine: Engine, start_id: int, end_id: int) -> pd.DataFrame:
    return pd.read_sql(
        text(f"""
            SELECT *
            FROM {STG_TABLE}
            WHERE stg_id BETWEEN :start_id AND :end_id
            ORDER BY stg_id
        """),
        engine,
        params={"start_id": start_id, "end_id": end_id},
    )


def transform_chunk_to_fact(chunk: pd.DataFrame, lookups: DimLookups) -> pd.DataFrame:
    """Wide -> long, then aggregate to reduce duplicates inside this chunk."""
    if chunk.empty:
        return chunk

    # normalize codes
    for col in ["dms_orig", "dms_dest", "fr_orig", "fr_dest", "dms_mode", "sctg2", "trade_type", "dist_band"]:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(norm_code)

    # zone keys
    chunk["origin_zone_key"] = chunk.apply(lambda r: make_zone_key(r.get("dms_orig"), r.get("fr_orig")), axis=1)
    chunk["dest_zone_key"] = chunk.apply(lambda r: make_zone_key(r.get("dms_dest"), r.get("fr_dest")), axis=1)

    # map to IDs
    chunk["origin_zone_id"] = chunk["origin_zone_key"].map(lookups.zone)
    chunk["destination_zone_id"] = chunk["dest_zone_key"].map(lookups.zone)
    chunk["mode_id"] = chunk["dms_mode"].map(lookups.mode)
    chunk["commodity_id"] = chunk["sctg2"].map(lookups.commodity)
    chunk["trade_type_id"] = chunk["trade_type"].map(lookups.trade_type)
    chunk["dist_band_id"] = chunk["dist_band"].map(lookups.dist_band)

    required = ["origin_zone_id", "destination_zone_id", "mode_id", "commodity_id", "trade_type_id", "dist_band_id"]
    chunk = chunk.dropna(subset=required).copy()

    base_cols = required
    out_parts: List[pd.DataFrame] = []

    for y in YEARS:
        cols = {
            "tons": f"tons_{y}",
            "value": f"value_{y}",
            "current_value": f"current_value_{y}",
            "tmiles": f"tmiles_{y}",
        }
        missing = [c for c in cols.values() if c not in chunk.columns]
        if missing:
            raise ValueError(f"Missing expected year columns in staging: {missing}")

        dfy = chunk[base_cols + list(cols.values())].copy()
        dfy.rename(columns={v: k for k, v in cols.items()}, inplace=True)
        dfy["year"] = y

        # drop all-null year rows
        all_null = dfy[["tons", "value", "current_value", "tmiles"]].isna().all(axis=1)
        dfy = dfy.loc[~all_null]

        out_parts.append(dfy)

    fact = pd.concat(out_parts, ignore_index=True)
    if fact.empty:
        return fact

    # enforce types
    for c in base_cols + ["year"]:
        fact[c] = fact[c].astype(int)

    # measures numeric
    for m in ["tons", "value", "current_value", "tmiles"]:
        fact[m] = pd.to_numeric(fact[m], errors="coerce")

    # aggregate inside chunk
    grain = ["origin_zone_id", "destination_zone_id", "mode_id", "commodity_id", "trade_type_id", "dist_band_id", "year"]
    before = len(fact)
    fact = (
        fact.groupby(grain, as_index=False)[["tons", "value", "current_value", "tmiles"]]
        .sum(min_count=1)
    )
    after = len(fact)
    if after < before:
        print(f"  Dedup/agg inside chunk: {before:,} -> {after:,} (collapsed {before-after:,} duplicates)")

    return fact


def reflect_fact_table(engine: Engine) -> Table:
    md = MetaData()
    return Table(FACT_TABLE, md, autoload_with=engine)


def upsert_fact(engine: Engine, fact_table: Table, fact_df: pd.DataFrame) -> int:
    """
    Insert fact rows with ON DUPLICATE KEY UPDATE that ADDS measures.
    This guarantees success even if duplicates exist across chunks.
    """
    if fact_df.empty:
        return 0

    # Convert NaN to None for IDs/year; for measures we prefer 0 for additive safety
    fact_df = fact_df.copy()
    for m in ["tons", "value", "current_value", "tmiles"]:
        fact_df[m] = fact_df[m].fillna(0.0)

    records = fact_df.to_dict(orient="records")

    ins = mysql_insert(fact_table)

    # Additive update with COALESCE protection
    upd = {
        "tons": func.coalesce(fact_table.c.tons, 0) + func.coalesce(ins.inserted.tons, 0),
        "value": func.coalesce(fact_table.c.value, 0) + func.coalesce(ins.inserted.value, 0),
        "current_value": func.coalesce(fact_table.c.current_value, 0) + func.coalesce(ins.inserted.current_value, 0),
        "tmiles": func.coalesce(fact_table.c.tmiles, 0) + func.coalesce(ins.inserted.tmiles, 0),
    }

    stmt = ins.on_duplicate_key_update(**upd)

    inserted_like = 0
    with engine.begin() as conn:
        for i in range(0, len(records), UPSERT_BATCH):
            batch = records[i:i + UPSERT_BATCH]
            conn.execute(stmt, batch)
            inserted_like += len(batch)

    # Note: "inserted_like" is number of rows we attempted, not net new rows.
    return len(fact_df)


def main():
    engine = get_engine()

    stg_count, max_id = get_staging_bounds(engine)
    if stg_count == 0 or max_id == 0:
        raise RuntimeError(f"{STG_TABLE} appears empty. Run staging load first.")

    expected_fact_rows = stg_count * len(YEARS)
    run_id = start_run_row(engine, phase="fact_build")

    # This is "rows produced (after in-chunk aggregation)". With UPSERT, some will become updates.
    produced_total = 0

    try:
        if TRUNCATE_FACT_AT_START:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {FACT_TABLE}"))

        lookups = load_dim_lookups(engine)
        fact_table = reflect_fact_table(engine)

        print(f"Staging rows: {stg_count:,}")
        print(f"Max stg_id:   {max_id:,}")
        print(f"Expected fact rows (upper bound): {expected_fact_rows:,}")
        print(f"Chunk size (stg_id ranges): {STG_ID_CHUNK:,}")
        print(f"UPSERT batch size: {UPSERT_BATCH:,}")

        start_id = 1
        while start_id <= max_id:
            end_id = min(start_id + STG_ID_CHUNK - 1, max_id)

            stg_chunk = fetch_staging_chunk(engine, start_id, end_id)
            fact_chunk = transform_chunk_to_fact(stg_chunk, lookups)
            upserted = upsert_fact(engine, fact_table, fact_chunk)

            produced_total += upserted

            print(
                f"Processed stg_id {start_id:,}..{end_id:,} | "
                f"stg_rows={len(stg_chunk):,} | fact_rows_produced={upserted:,} | total_produced={produced_total:,}"
            )

            start_id = end_id + 1

        # actual final count in DB (truth)
        final_cnt = int(pd.read_sql(text(f"SELECT COUNT(*) AS c FROM {FACT_TABLE}"), engine).loc[0, "c"])

        end_run_row(
            engine,
            run_id=run_id,
            status="SUCCESS",
            notes=f"Fact build completed successfully. final_fact_count={final_cnt:,}",
            expected_fact_rows=expected_fact_rows,
            fact_row_count=final_cnt,
        )

        print("Fact build completed successfully.")
        print(f"Final fact row count (DB): {final_cnt:,}")

    except Exception as e:
        # store short note (not the whole SQL dump)
        end_run_row(
            engine,
            run_id=run_id,
            status="FAILED",
            notes=repr(e),
            expected_fact_rows=expected_fact_rows,
            fact_row_count=None,
        )
        raise


if __name__ == "__main__":
    main()
