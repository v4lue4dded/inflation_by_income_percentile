"""
build_duckdb.py
Create/overwrite a DuckDB database containing the BLS‑series observations that
were previously downloaded into data/raw/requests/*.json
"""

from pathlib import Path
import json
import pandas as pd
import duckdb
import re

# ────────────────────────────────────────────────────────────────────────────────
# Paths
# ────────────────────────────────────────────────────────────────────────────────
main_folder   = Path(__file__).resolve().parent
raw_dir       = main_folder / "data" / "raw"
requests_dir  = main_folder / "data" / "raw" / "requests"
db_path       = main_folder / "data" / "processing" / "inflation.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)      # be sure the data/ folder exists

# ────────────────────────────────────────────────────────────────────────────────
# Collect every observation from every JSON file
# ────────────────────────────────────────────────────────────────────────────────
records = []

for fp in requests_dir.glob("*.json"):
    with fp.open(encoding="utf‑8") as f:
        payload = json.load(f)

    # A batch file can contain more than one series, so loop defensively
    for series in payload["Results"]["series"]:
        sid = series["seriesID"]

        for obs in series["data"]:
            # BLS puts the year outside the period code; keep only what you asked for
            footnotes = obs.get("footnotes", [])
            # Turn the list of dicts into a single semicolon‑separated string
            footnote_text = "; ".join(
                fn.get("text", "").strip() for fn in footnotes if fn and fn.get("text")
            )

            records.append(
                {
                    "seriesID":  sid,
                    "period":    obs["year"],          # store the calendar year
                    "periodName": obs["periodName"],   # e.g. "Annual"
                    "value":     obs["value"],
                    "footnotes": footnote_text,
                }
            )

# Put everything into a DataFrame
df_series = pd.DataFrame(records)
df_series["value"] = pd.to_numeric(df_series["value"], errors="coerce")

# ────────────────────────────────────────────────────────────────────────────────
# Write to DuckDB
# ────────────────────────────────────────────────────────────────────────────────
con = duckdb.connect(db_path)

# Re‑create the table from scratch each time the script runs
con.execute("DROP TABLE IF EXISTS series_import;")

# Register the DataFrame as a temporary view and create the permanent table
con.register("records_df", df_series)
con.execute(
    """
    CREATE OR REPLACE TABLE series_import AS
    SELECT
        seriesID  :: VARCHAR as seriesID
      , period    :: VARCHAR as period
      , periodName:: VARCHAR as periodName
      , value     :: DOUBLE as value
      , footnotes :: VARCHAR as footnotes
    FROM records_df
    """
)


# reopen the same database or create it if it doesn't exist yet
con = duckdb.connect(db_path)           # db_path defined earlier (e.g. data/series.duckdb)

for fp in raw_dir.iterdir():
    # skip directories
    if fp.is_dir():
        continue

    # read as TSV and strip whitespace from headers & string cells
    df_tsv = (
        pd.read_csv(fp, sep="\t")
          .apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    )
    df_tsv.columns = df_tsv.columns.str.strip()

    # derive a legal SQL table name → file stem with dots changed to underscores
    table_name = re.sub(r"\W+", "_", fp.name).lower()

    # replace existing table each run so things stay in sync with the files
    con.execute(f"DROP TABLE IF EXISTS {table_name};")
    con.register("tmp_df_tsv", df_tsv)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_df_tsv;")
    con.unregister("tmp_df_tsv")

    print(f"✅  Imported {fp.name:15} ➜ table '{table_name}' ({len(df_tsv):,} rows)")

con.close()


con.close()
print(f"✅  {len(df_tsv):,} rows written to {db_path} (table: series_import)")
