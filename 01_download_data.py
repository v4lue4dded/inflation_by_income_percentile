import requests
import pandas as pd
import time
from pathlib import Path
import os
import json

# Get the directory where this script is located
main_folder = Path(__file__).resolve().parent

# Create raw data folder
output_dir = main_folder / "data" / "raw"
output_dir.mkdir(parents=True, exist_ok=True)

# Load CE series definitions
# Downloaded from: https://download.bls.gov/pub/time.series/cx/cx.series
series_df = pd.read_csv(output_dir / "cx.series", sep="\t")
series_df.columns = series_df.columns.str.strip()
series_df["series_id"] = series_df["series_id"].str.strip()

# Load characteristic codes (income quintiles)
# Downloaded from: https://download.bls.gov/pub/time.series/cx/cx.characteristics
char_df = pd.read_csv(
    output_dir / "cx.characteristics",
    sep="\t", names=[
        "demographics_code", "characteristics_code", "characteristics_text",
        "display_level", "selectable", "sort_sequence"
    ])
char_df.columns = char_df.columns.str.strip()

# Filter for average annual expenditures by income quintile
income_quintile_codes = ['02', '03', '04', '05', '06']
filtered_series = series_df[
    (series_df["category_code"].str.strip() == "EXPEND") &    # only expenditures
    (series_df["process_code"].str.strip()  == "M")      &    # mean dollars
    (series_df["demographics_code"].str.strip() == "LB01") &  # income‑before‑tax table
    (series_df["characteristics_code"].isin(income_quintile_codes))
].copy()

# Save filtered series for inspection
(filtered_series
 .to_csv(main_folder / "data" / "processing" / "filtered_series.tsv", 
         sep="\t", encoding="utf-8"))

# Define 10-year ranges (BLS API limitation)
year_ranges = [(start, min(start + 9, 2025)) for start in range(1984, 2026, 10)]

# Download and save each response as JSON
for i_series_id in filtered_series['series_id'].unique():
    for start, end in year_ranges:
        out_file = output_dir / "requests" / f"{i_series_id}_{start}_{end}.json"
        if out_file.exists():
            print(f"⏭️ Skipping (already exists): {out_file.name}")
            continue

        url = f"https://api.bls.gov/publicAPI/v2/timeseries/data/{i_series_id}"
        params = {"startyear": start, "endyear": end}
        r = requests.get(url, params=params)

        if r.status_code == 200:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(r.json(), f, ensure_ascii=False, indent=2)
            print(f"✅ Saved JSON: {out_file.name}")
        else:
            print(f"❌ Failed: {i_series_id} ({start}-{end}) — HTTP {r.status_code}")
        
        # time.sleep(0.)
