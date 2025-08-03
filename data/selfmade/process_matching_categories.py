import pandas as pd

# ---- Load and order the data -------------------------------------------------
# The input must have columns: ID, level, and "Expenditure cateogories with spaces"
df = pd.read_csv("my_matching_categories.csv", sep='\t')
df = df.sort_values("ID").reset_index(drop=True)

# ---- Prepare output columns --------------------------------------------------
# We'll create level_0 ... level_4 and fill them as we iterate
for j in range(5):
    df[f"level_{j}"] = ""

# This will store the current path (what we've seen so far at each level)
current = [""] * 5

# ---- Build the hierarchy row-by-row ------------------------------------------
for i in range(len(df)):
    L = int(df.at[i, "level"])
    label = df.at[i, "Expenditure cateogories with spaces"]

    # Put the current row's label at its level
    current[L] = label

    # Anything deeper than L is no longer in scope for this branch
    for j in range(L + 1, 5):
        current[j] = label

    # Write the full path for this row
    for j in range(5):
        df.at[i, f"level_{j}"] = current[j]

# ---- Save/inspect ------------------------------------------------------------
df.to_csv("my_matching_categories_with_levels.csv", index=False, sep='\t')

df
