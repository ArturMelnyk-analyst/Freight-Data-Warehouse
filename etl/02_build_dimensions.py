"""
PR#2 — Phase 2 Dimensions (DDL + deterministic loaders + run logging)

Run from project root:
    python -m etl.02_build_dimensions

What it does:
- Ensures run-log + dim tables exist (DDL in sql/ddl)
- Reads official FAF metadata workbook (data/metadata/faf/FAF5_metadata.xlsx)
- Builds and loads:
    dim_zone (DMS + FR unified)
    dim_mode
    dim_commodity
    dim_trade_type
    dim_distance_band
    dim_year (2018–2024)
- Idempotent: TRUNCATE dims then reload (safe to rerun)
- Writes SUCCESS/FAILED + counts into etl_run_log
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from etl.utils.db import get_engine

# ----------------------------
# Paths
# ----------------------------
META_XLSX = os.path.join("data", "metadata", "faf", "FAF5_metadata.xlsx")

DDL_DIM_TABLES = os.path.join("sql", "ddl", "01_dim_tables.sql")
DDL_RUN_LOG = os.path.join("sql", "ddl", "99_etl_run_log.sql")


# ----------------------------
# SQL helpers
# ----------------------------
def _read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def execute_sql_file(engine, path: str) -> None:
    """
    Executes a .sql file (simple splitter on ';' for controlled DDL scripts).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql = _read_sql(path).strip()
    if not sql:
        raise ValueError(f"SQL file is empty: {path}")

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def truncate_tables(engine, tables: list[str]) -> None:
    """
    Idempotency: safe to rerun.
    """
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        for t in tables:
            conn.execute(text(f"TRUNCATE TABLE {t};"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))


def insert_df(engine, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        print(f"[WARN] {table}: dataframe empty; skipping insert.")
        return

    df.to_sql(
        table,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


# ----------------------------
# Run log helpers
# ----------------------------
def init_run_log(engine, run_id: str, phase: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_run_log (run_id, phase, status, start_ts)
                VALUES (:run_id, :phase, 'RUNNING', :start_ts)
                """
            ),
            {"run_id": run_id, "phase": phase, "start_ts": datetime.utcnow()},
        )


def finish_run_success(engine, run_id: str, counts: dict[str, int]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl_run_log
                SET status='SUCCESS',
                    end_ts=:end_ts,
                    dim_zone_count=:dim_zone_count,
                    dim_mode_count=:dim_mode_count,
                    dim_commodity_count=:dim_commodity_count,
                    dim_trade_type_count=:dim_trade_type_count,
                    dim_distance_band_count=:dim_distance_band_count,
                    dim_year_count=:dim_year_count
                WHERE run_id=:run_id
                """
            ),
            {
                "run_id": run_id,
                "end_ts": datetime.utcnow(),
                "dim_zone_count": counts["dim_zone_count"],
                "dim_mode_count": counts["dim_mode_count"],
                "dim_commodity_count": counts["dim_commodity_count"],
                "dim_trade_type_count": counts["dim_trade_type_count"],
                "dim_distance_band_count": counts["dim_distance_band_count"],
                "dim_year_count": counts["dim_year_count"],
            },
        )


def finish_run_failed(engine, run_id: str, error_msg: str) -> None:
    notes = (error_msg or "")[:255]
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl_run_log
                SET status='FAILED',
                    end_ts=:end_ts,
                    notes=:notes
                WHERE run_id=:run_id
                """
            ),
            {"run_id": run_id, "end_ts": datetime.utcnow(), "notes": notes},
        )


# ----------------------------
# Metadata builders (explicit mappings)
# ----------------------------
def build_dim_zone(dom: pd.DataFrame, fr: pd.DataFrame) -> pd.DataFrame:
    """
    Domestic sheet columns:
      ['Numeric Label', 'Short Description', 'Long Description']

    Foreign sheet columns:
      ['Numeric Label', 'Description']

    We map:
      zone_code = Numeric Label
      zone_name = (Domestic: Long Description) / (Foreign: Description)
    """
    # Domestic mapping
    if "Numeric Label" not in dom.columns or "Long Description" not in dom.columns:
        raise ValueError(
            "dim_zone (Domestic): expected columns {'Numeric Label', 'Long Description'}. "
            f"Got: {list(dom.columns)}"
        )

    dz_dom = dom[["Numeric Label", "Long Description"]].rename(
        columns={"Numeric Label": "zone_code", "Long Description": "zone_name"}
    )
    dz_dom["zone_type"] = "DMS"

    # Foreign mapping
    if "Numeric Label" not in fr.columns or "Description" not in fr.columns:
        raise ValueError(
            "dim_zone (Foreign): expected columns {'Numeric Label', 'Description'}. "
            f"Got: {list(fr.columns)}"
        )

    dz_fr = fr[["Numeric Label", "Description"]].rename(
        columns={"Numeric Label": "zone_code", "Description": "zone_name"}
    )
    dz_fr["zone_type"] = "FR"

    dim_zone = pd.concat([dz_dom, dz_fr], ignore_index=True)

    dim_zone["zone_code"] = dim_zone["zone_code"].astype(str).str.strip()
    dim_zone["zone_name"] = dim_zone["zone_name"].astype(str).str.strip()

    dim_zone = dim_zone[(dim_zone["zone_code"] != "") & (dim_zone["zone_code"] != "nan")]
    dim_zone = dim_zone.drop_duplicates(subset=["zone_type", "zone_code"])

    return dim_zone[["zone_type", "zone_code", "zone_name"]]



def build_dim_from_standard_sheet(
    df: pd.DataFrame,
    code_col: str,
    name_col: str,
    out_code: str,
    out_name: str,
) -> pd.DataFrame:
    """
    FAF metadata tabs vary slightly:
      - Some have 'Long Description'
      - Some have 'Description'
      - Some have 'Short Description'

    We keep it simple:
      - code_col is usually 'Numeric Label'
      - name_col is preferred, but we fallback gracefully.
    """
    if code_col not in df.columns:
        raise ValueError(f"Expected code column '{code_col}' not found. Got: {list(df.columns)}")

    # Prefer requested name_col, but allow common fallbacks
    if name_col not in df.columns:
        fallback_order = ["Long Description", "Description", "Short Description"]
        found = None
        for cand in fallback_order:
            if cand in df.columns:
                found = cand
                break
        if found is None:
            raise ValueError(
                f"Expected name column '{name_col}' not found and no fallback available. "
                f"Got: {list(df.columns)}"
            )
        name_col = found  # use fallback

    out = df[[code_col, name_col]].rename(columns={code_col: out_code, name_col: out_name}).copy()
    out[out_code] = out[out_code].astype(str).str.strip()
    out[out_name] = out[out_name].astype(str).str.strip()
    out = out[(out[out_code] != "") & (out[out_code] != "nan")]
    out = out.drop_duplicates(subset=[out_code])
    return out[[out_code, out_name]]



# ----------------------------
# Main
# ----------------------------
def main() -> None:
    engine = get_engine()
    run_id = str(uuid.uuid4())
    phase = "dimensions"

    # Ensure schema exists
    execute_sql_file(engine, DDL_RUN_LOG)
    execute_sql_file(engine, DDL_DIM_TABLES)

    init_run_log(engine, run_id, phase)

    try:
        # Load metadata workbook
        dom = pd.read_excel(META_XLSX, sheet_name="FAF Zone (Domestic)")
        fr = pd.read_excel(META_XLSX, sheet_name="FAF Zone (Foreign)")
        comm = pd.read_excel(META_XLSX, sheet_name="Commodity (SCTG2)")
        modes = pd.read_excel(META_XLSX, sheet_name="Mode")
        trade = pd.read_excel(META_XLSX, sheet_name="Trade Type")
        dist = pd.read_excel(META_XLSX, sheet_name="Distance Band")

        # Build dims (explicit mapping to your sheet headers)
        dim_zone = build_dim_zone(dom, fr)

        dim_mode = build_dim_from_standard_sheet(
            modes,
            code_col="Numeric Label",
            name_col="Description",
            out_code="mode_code",
            out_name="mode_name",
        )

        dim_commodity = build_dim_from_standard_sheet(
            comm,
            code_col="Numeric Label",
            name_col="Long Description",
            out_code="sctg2",
            out_name="commodity_name",
        )

        dim_trade_type = build_dim_from_standard_sheet(
            trade,
            code_col="Numeric Label",
            name_col="Long Description",
            out_code="trade_type_code",
            out_name="trade_type_name",
        )

        dim_distance_band = build_dim_from_standard_sheet(
            dist,
            code_col="Numeric Label",
            name_col="Long Description",
            out_code="dist_band_code",
            out_name="dist_band_name",
        )

        dim_year = pd.DataFrame({"year": [2018, 2019, 2020, 2021, 2022, 2023, 2024]})

        # Idempotency: clear then reload
        truncate_tables(engine, [
            "dim_zone",
            "dim_mode",
            "dim_commodity",
            "dim_trade_type",
            "dim_distance_band",
            "dim_year",
        ])

        # Load
        insert_df(engine, dim_zone, "dim_zone")
        insert_df(engine, dim_mode, "dim_mode")
        insert_df(engine, dim_commodity, "dim_commodity")
        insert_df(engine, dim_trade_type, "dim_trade_type")
        insert_df(engine, dim_distance_band, "dim_distance_band")
        insert_df(engine, dim_year, "dim_year")

        counts = {
            "dim_zone_count": int(len(dim_zone)),
            "dim_mode_count": int(len(dim_mode)),
            "dim_commodity_count": int(len(dim_commodity)),
            "dim_trade_type_count": int(len(dim_trade_type)),
            "dim_distance_band_count": int(len(dim_distance_band)),
            "dim_year_count": int(len(dim_year)),
        }

        print("Loaded dimension counts:")
        for k, v in counts.items():
            print(f"  {k}: {v}")

        finish_run_success(engine, run_id, counts)

    except Exception as e:
        finish_run_failed(engine, run_id, str(e))
        raise


if __name__ == "__main__":
    main()
