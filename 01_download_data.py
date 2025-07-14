import requests

import pandas as pd
import time
from pathlib import Path
from os.path import join as opj
import os

# Create a folder to store data

import os

main_folder = os.path.dirname(__file__)

output_dir = opj("data","raw")
output_dir.mkdir(exist_ok=True)

# Load CE series definitions
# downloaded from:
# https://download.bls.gov/pub/time.series/cx/cx.series
series_df = pd.read_csv(
    opj(output_dir,"cx.series"), sep="\t")

# Load characteristic codes (to identify income quintiles)
# downloaded from:
# https://download.bls.gov/pub/time.series/cx/cx.characteristics
char_df = pd.read_csv(
    opj(output_dir,"cx.characteristics"),
    sep="\t", names=[
        "demographics_code", "characteristics_code", "characteristics_text",
        "display_level", "selectable", "sort_sequence"
    ])

# Filter for series that are:
# - Mean spending values (code ends with 'M')
# - Belong to income quintiles (codes 02–06)
income_quintile_codes = ['02', '03', '04', '05', '06']
filtered_series = series_df[
    series_df['series_id'].str.endswith('M') &
    series_df['series_id'].str[12:14].isin(income_quintile_codes)
]

# Time chunks: BLS API max = 10 years per call
year_ranges = [(start, min(start + 9, 2025)) for start in range(1984, 2026, 10)]

requests_dir = opj(output_dir, "requests")

# Function to download and save one series chunk
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
    time.sleep(0.5)  # Throttle requests to avoid hitting BLS limits

# Run the loop to download all filtered series
for sid in filtered_series['series_id'].unique():
    for start, end in year_ranges:
        fetch_and_save_series(sid, start, end)
