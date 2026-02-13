import pandas as pd
from pathlib import Path
from sqlalchemy import text
from etl.utils.db import get_engine
from etl.utils.file_fingerprint import get_file_metadata
from etl.run_log import start_run, end_run

CHUNK_SIZE = 100_000
CSV_PATH = Path("data/raw/faf/FAF5.7.1_2018-2024.csv")


def main():
    engine = get_engine()

    if not CSV_PATH.exists():
        raise FileNotFoundError("CSV file not found. Place it in data/raw/faf/")

    file_meta = get_file_metadata(CSV_PATH)

    run_id = start_run(engine, process_name="staging_load")

    total_rows = 0

    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE stg_faf"))

        for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
            chunk.to_sql(
                "stg_faf",
                con=engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            total_rows += len(chunk)
            print(f"Loaded {total_rows} rows...")

        end_run(
            engine,
            run_id=run_id,
            status="SUCCESS",
            source_file=file_meta["file_name"],
            file_hash=file_meta["file_hash"],
            file_modified_ts=file_meta["file_modified_ts"],
            stg_row_count=total_rows
        )

        print("Staging load completed successfully.")

    except Exception as e:
        end_run(engine, run_id=run_id, status="FAILED", error_note=str(e))
        raise


if __name__ == "__main__":
    main()
