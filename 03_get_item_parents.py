from pathlib import Path
import json
import pandas as pd
import duckdb
import re

# ────────────────────────────────────────────────────────────────────────────────
# Paths (BLS = Bureau of Labor Statistics)
# ────────────────────────────────────────────────────────────────────────────────
main_folder  = Path(__file__).resolve().parent
raw_dir      = main_folder / "data" / "raw"
requests_dir = raw_dir / "requests"
db_path      = main_folder / "data" / "processing" / "inflation.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Hierarchy helpers
# ────────────────────────────────────────────────────────────────────────────────
def add_parent(df, id_col="item_code"):
    """
    Adds a 'parent_code' column based on display_level & sort_sequence.
    Assumes parents appear earlier in ascending sort_sequence.
    """
    df = df.sort_values("sort_sequence").reset_index(drop=True)
    last_at_level = {}
    parents = []
    for _, row in df.iterrows():
        lvl = int(row.display_level)
        parents.append(last_at_level.get(lvl - 1))
        last_at_level[lvl] = row[id_col]
    df["parent_code"] = parents
    return df

def add_level_columns(df, id_col="item_code", name_col="item_name",
                      parent_col="parent_code", fill_empty=""):
    """
    Expands each row into level_0..level_k (root..self). Deeper levels blank.
    """
    parent_map = dict(zip(df[id_col], df[parent_col]))
    name_map   = dict(zip(df[id_col], df[name_col]))
    chain_cache = {}

    def chain(code):
        if code in chain_cache:
            return chain_cache[code]
        seq = []
        cur = code
        while cur:
            seq.append(cur)
            cur = parent_map.get(cur)
        seq = list(reversed(seq))        # root → self
        chain_cache[code] = seq
        return seq

    max_depth = 0
    chains = {}
    for code in df[id_col]:
        c = chain(code)
        chains[code] = c
        if len(c) > max_depth:
            max_depth = len(c)

    # Create empty columns
    for lvl in range(max_depth):
        df[f"level_{lvl}"] = fill_empty

    # Fill
    for i, row in df.iterrows():
        seq = chains[row[id_col]]
        for lvl, code in enumerate(seq):
            df.at[i, f"level_{lvl}"] = name_map[code]

    return df, max_depth

def add_path_column(df, max_depth):
    """Optional: concatenate non-empty level_* into a 'path' column."""
    level_cols = [f"level_{i}" for i in range(max_depth)]
    df["path"] = df[level_cols].replace("", pd.NA).apply(
        lambda r: " > ".join([x for x in r if pd.notna(x)]), axis=1
    )
    return df

# ────────────────────────────────────────────────────────────────────────────────
# Load cu (Consumer Price Index) items
# ────────────────────────────────────────────────────────────────────────────────
cu_items = pd.read_csv(raw_dir / "cu.item", sep="\t", dtype=str)
cu_items["display_level"] = cu_items["display_level"].astype(int)
cu_items["sort_sequence"] = cu_items["sort_sequence"].astype(int)

cu_with_parent = add_parent(cu_items.copy())
cu_with_levels, cu_depth = add_level_columns(cu_with_parent, name_col="item_name")
cu_with_levels = add_path_column(cu_with_levels, cu_depth)

# ────────────────────────────────────────────────────────────────────────────────
# Load CX (Consumer Expenditure) items
# ────────────────────────────────────────────────────────────────────────────────
cx_items = pd.read_csv(raw_dir / "cx.item", sep="\t", dtype=str)
cx_items["display_level"] = cx_items["display_level"].astype(int)
cx_items["sort_sequence"] = cx_items["sort_sequence"].astype(int)

# Global hierarchy (change to groupby subcategory_code if desired)
cx_with_parent = add_parent(cx_items.copy())
cx_with_levels, cx_depth = add_level_columns(cx_with_parent, name_col="item_text")
cx_with_levels = add_path_column(cx_with_levels, cx_depth)

# ────────────────────────────────────────────────────────────────────────────────
# Persist to DuckDB
# ────────────────────────────────────────────────────────────────────────────────
con = duckdb.connect(db_path)

# Write / replace cu hierarchy table
con.execute("DROP TABLE IF EXISTS cu_item_hierarchy;")
con.register("tmp_cu", cu_with_levels)
con.execute("CREATE TABLE cu_item_hierarchy AS SELECT * FROM tmp_cu;")
con.unregister("tmp_cu")

# Write / replace CE hierarchy table
con.execute("DROP TABLE IF EXISTS cx_item_hierarchy;")
con.register("tmp_ce", cx_with_levels)
con.execute("CREATE TABLE cx_item_hierarchy AS SELECT * FROM tmp_ce;")
con.unregister("tmp_ce")

con.close()

print("✅ Loaded tables into DuckDB:")
print("  - cu_item_hierarchy")
print("  - cx_item_hierarchy")
