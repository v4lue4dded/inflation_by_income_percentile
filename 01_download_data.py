import requests
import pandas as pd
import time
from pathlib import Path
import json

# ────────────────────────────────────────────────────────────────────────────────
# Paths and folders
# ────────────────────────────────────────────────────────────────────────────────
main_folder  = Path(__file__).resolve().parent
output_dir   = main_folder / "data" / "raw"
requests_dir = output_dir / "requests"

output_dir.mkdir(parents=True, exist_ok=True)
requests_dir.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Load BLS API key from private/keys.json
# ────────────────────────────────────────────────────────────────────────────────
key_path = main_folder / "private" / "keys.json"
with open(key_path, encoding="utf-8") as f:
    bls_api_key = json.load(f)["bls_api_key"].strip()

# ────────────────────────────────────────────────────────────────────────────────
# Load CE series metadata
# ────────────────────────────────────────────────────────────────────────────────
series_df = pd.read_csv(output_dir / "cx.series", sep="\t").apply(lambda col: col.str.strip() if col.dtype == "object" else col)
series_df.columns = series_df.columns.str.strip()

# Filter: average annual expenditures by income‑before‑tax quintile
income_quintile_codes = ['02', '03', '04', '05', '06']
filtered_series = series_df[
    (series_df["category_code"]      == "EXPEND") &
    (series_df["process_code"]       == "M")      &
    (series_df["demographics_code"]  == "LB01")   &
    (series_df["characteristics_code"].isin(income_quintile_codes))
]

# ────────────────────────────────────────────────────────────────────────────────
# BLS API batch download  |  50 series × 20 years per call  (v2.0, key required)
# ────────────────────────────────────────────────────────────────────────────────


# Build a list of 20‑year windows (1984‑2003, 2004‑2023, 2024‑2025)
year_ranges = [(start, min(start + 19, 2025)) for start in range(1984, 2026, 20)]

# All unique series IDs we need
series_list = filtered_series['series_id'].unique().tolist()

for start, end in year_ranges:

    # Identify series still missing for this window
    remaining = [
        sid for sid in series_list
        if not (requests_dir / f"{sid}_{start}_{end}.json").exists()
    ]

    if not remaining:
        continue
    # ── 1. PRE‑BUILD all batches (≤ 50 series each) ───────────────────────────
    batch_size = 50
    batches = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]
    # ──────────────────────────────────────────────────────────────────────────

    # ── 2. Iterate over the prepared list of batches ─────────────────────────
    for batch in batches:
        print(batch)

        payload = {
            "seriesid": batch,
            "startyear": start,
            "endyear": end,
            "registrationkey": bls_api_key
        }

        resp = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=payload,
            timeout=30
        )
        print(resp)

        if resp.status_code == 200 and resp.json().get("status") == "REQUEST_SUCCEEDED":
            for s in resp.json()["Results"]["series"]:
                sid = s["seriesID"]
                out_file = requests_dir / f"{sid}_{start}_{end}.json"
                # save each series as its own file
                out_file.write_text(json.dumps({"Results": {"series": [s]}}, indent=2),
                                    encoding="utf-8")
                print(f"✅ {out_file.name}")
        else:
            print(f"❌ Batch {start}-{end} HTTP {resp.status_code}")
            print(resp.text[:400])

        time.sleep(0.25)  # stay below 50 requests / 10 s   

