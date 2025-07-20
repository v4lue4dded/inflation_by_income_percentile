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



def add_parent(df, id_col="item_code"):
    """
    Adds a 'parent_code' column based on display_level & sort_sequence.
    """
    df = df.sort_values("sort_sequence").reset_index(drop=True)
    last_at_level = {}
    parent_codes = []

    for _, row in df.iterrows():
        lvl = int(row.display_level)
        parent = last_at_level.get(lvl - 1)  # None for top level
        parent_codes.append(parent)
        last_at_level[lvl] = row[id_col]

    df["parent_code"] = parent_codes
    return df

def add_level_columns(df, id_col="item_code", name_col="item_name",
                      parent_col="parent_code", fill_empty=""):
    """
    For each row, build ancestor chain (root ... self) and expand into
    level_0, level_1, ..., level_{max_depth-1}. Current item occupies the
    last non-empty level for that row. Deeper levels are left empty.
    """
    # Build quick lookups
    parent_map = dict(zip(df[id_col], df[parent_col]))
    name_map   = dict(zip(df[id_col], df[name_col]))

    # Cache chains to avoid re-walking parents repeatedly
    chain_cache = {}

    def chain(code):
        if code in chain_cache:
            return chain_cache[code]
        path = []
        cur = code
        while cur:
            path.append(cur)
            cur = parent_map.get(cur)
        # path now: [self, parent, grandparent, ...]; reverse to root→self
        full = list(reversed(path))
        chain_cache[code] = full
        return full

    # Build all chains, track max depth
    chains = {}
    max_depth = 0
    for code in df[id_col]:
        c = chain(code)
        chains[code] = c
        if len(c) > max_depth:
            max_depth = len(c)

    # Prepare columns
    for lvl in range(max_depth):
        df[f"level_{lvl}"] = fill_empty

    # Fill columns with names
    for idx, row in df.iterrows():
        codes_chain = chains[row[id_col]]
        for lvl, code in enumerate(codes_chain):
            df.at[idx, f"level_{lvl}"] = name_map[code]

    return df, max_depth

# --- CPI example ---
cpi_items = pd.read_csv("data/raw/cu.item", sep="\t", dtype=str)
cpi_items["display_level"] = cpi_items["display_level"].astype(int)
cpi_items["sort_sequence"] = cpi_items["sort_sequence"].astype(int)

cpi_with_parent = add_parent(cpi_items)
cpi_with_levels, cpi_depth = add_level_columns(cpi_with_parent)

# --- CE example ---
ce_items = pd.read_csv("data/raw/cx.item", sep="\t", dtype=str)
ce_items["display_level"] = ce_items["display_level"].astype(int)
ce_items["sort_sequence"] = ce_items["sort_sequence"].astype(int)

# If hierarchy must be confined within subcategory_code groups, group first; else do global.
# Here we assume a global hierarchy analogous to CPI; if not, uncomment the groupby approach.

# Global approach:
ce_with_parent = add_parent(ce_items)
ce_with_levels, ce_depth = add_level_columns(ce_with_parent, name_col='item_text')

# Example: inspect a few rows
print(cpi_with_levels.filter(regex="^level_").head())
print(ce_with_levels.filter(regex="^level_").head())

# reopen the same database or create it if it doesn't exist yet
con = duckdb.connect(db_path)           # db_path defined earlier (e.g. data/series.duckdb)
con.execute(f"DROP TABLE IF EXISTS {table_name};")
con.register("tmp_df_tsv", df_tsv)
con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_df_tsv;")
con.unregister("tmp_df_tsv")
con.close()
