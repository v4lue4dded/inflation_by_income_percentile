from pathlib import Path
import json
import pandas as pd
import duckdb
import re

# ────────────────────────────────────────────────────────────────────────────────
# Paths
# ────────────────────────────────────────────────────────────────────────────────
main_folder   = Path(__file__).resolve().parent
bls_dir       = main_folder / "data" / "raw" / "bureau_of_labor_statistics_tables"
requests_dir  = main_folder / "data" / "raw" / "requests"
my_matching_categories             = main_folder / "data" / "selfmade" / "my_matching_categories.csv"
my_matching_categories_with_levels = main_folder / "data" / "selfmade" / "my_matching_categories_with_levels.csv"
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

for fp in bls_dir.iterdir():
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



# read as TSV and strip whitespace from headers & string cells
df_my_matching_categories = (
pd.read_csv(my_matching_categories, sep="\t"))
df_my_matching_categories.columns = df_my_matching_categories.columns.str.strip()
df_my_matching_categories = df_my_matching_categories.sort_values("ID").reset_index(drop=True)

# ---- Prepare output columns --------------------------------------------------
# We'll create level_0 ... level_4 and fill them as we iterate
for j in range(5):
    df_my_matching_categories[f"level_{j}"] = ""

# This will store the current path (what we've seen so far at each level)
current = [""] * 5

# ---- Build the hierarchy row-by-row ------------------------------------------
for i in range(len(df_my_matching_categories)):
    L = int(df_my_matching_categories.at[i, "level"])
    label = df_my_matching_categories.at[i, "Expenditure categories with spaces"]

    # Put the current row's label at its level
    current[L] = label

    # Anything deeper than L is no longer in scope for this branch
    for j in range(L + 1, 5):
        current[j] = label

    # Write the full path for this row
    for j in range(5):
        df_my_matching_categories.at[i, f"level_{j}"] = current[j]

# ---- Save/inspect ------------------------------------------------------------
df_my_matching_categories.to_csv(my_matching_categories_with_levels, index=False, sep='\t')


# replace existing table each run so things stay in sync with the files
con.execute(f"DROP TABLE IF EXISTS my_matching_categories;")
con.register("tmp_df_my_matching_categories", df_my_matching_categories)
con.execute(f"CREATE TABLE my_matching_categories AS SELECT * FROM tmp_df_my_matching_categories;")
con.unregister("tmp_df_my_matching_categories")

con.close()