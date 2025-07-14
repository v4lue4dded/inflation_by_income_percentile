import requests
import pandas as pd
import time
from pathlib import Path
import os

# Get the directory where this script is located
main_folder = Path(__file__).resolve().parent

# Create raw data folder
output_dir = main_folder / "data" / "raw"
output_dir.mkdir(parents=True, exist_ok=True)

# Load CE series definitions
# Downloaded from: https://download.bls.gov/pub/time.series/cx/cx.series
series_df = pd.read_csv(output_dir / "cx.series", sep="\t")
series_df.columns = series_df.columns.str.strip()  # Trim column names

# Load characteristic codes (income quintiles)
# Downloaded from: https://download.bls.gov/pub/time.series/cx/cx.characteristics
char_df = pd.read_csv(
    output_dir / "cx.characteristics",
    sep="\t", names=[
        "demographics_code", "characteristics_code", "characteristics_text",
        "display_level", "selectable", "sort_sequence"
    ])
char_df.columns = char_df.columns.str.strip()

# Filter for mean spending series in income quintiles (02–06)
income_quintile_codes = ['02', '03', '04', '05', '06']
filtered_series = series_df[
    series_df['series_title'].str.contains("quintile", case=False, na=False)
]


# BLS API allows max 10 years per request
year_ranges = [(start, min(start + 9, 2025)) for start in range(1984, 2026, 10)]

# Function to download and save a series
def fetch_and_save_series(sid, start, end):
    url = f"https://api.bls.gov/publicAPI/v2/timeseries/data/{sid}"
    params = {"startyear": start, "endyear": end}
    r = requests.get(url, params=params)
    if r.status_code == 200:
        try:
            data = r.json()['Results']['series'][0]['data']
            df = pd.json_normalize(data)
            out_file = output_dir / f"{sid}_{start}_{end}.csv"
            df.to_csv(out_file, index=False)
            print(f"✅ Saved: {out_file.name}")
        except (KeyError, IndexError):
            print(f"⚠️  No data for {sid} ({start}-{end})")
    else:
        print(f"❌ Failed for {sid} ({start}-{end})")
    time.sleep(0.5)

# Run the loop
for sid in filtered_series['series_id'].unique():
    for start, end in year_ranges:
        fetch_and_save_series(sid, start, end)
