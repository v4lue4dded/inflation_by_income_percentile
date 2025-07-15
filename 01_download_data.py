import requests
import pandas as pd
import time
from pathlib import Path
import json

# ────────────────────────────────────────────────────────────────────────────────
# Paths and folders (BLS = Bureau of Labor Statistics)
# ────────────────────────────────────────────────────────────────────────────────
main_folder  = Path(__file__).resolve().parent
output_dir   = main_folder / "data" / "raw"
requests_dir = output_dir / "requests"
output_dir.mkdir(parents=True, exist_ok=True)
requests_dir.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Load API key (BLS) from private/keys.json
# ────────────────────────────────────────────────────────────────────────────────
with open(main_folder / "private" / "keys.json", encoding="utf-8") as f:
    bls_api_key = json.load(f)["bls_api_key"].strip()

# ────────────────────────────────────────────────────────────────────────────────
# Load CE (Consumer Expenditure) series metadata
# ────────────────────────────────────────────────────────────────────────────────
ce_series_df = (
    pd.read_csv(output_dir / "cx.series", sep="\t")
      .apply(lambda col: col.str.strip() if col.dtype == "object" else col)
)
ce_series_df.columns = ce_series_df.columns.str.strip()

# Filter CE: average annual expenditures by income‑before‑tax quintile
quints = ['02','03','04','05','06']
ce_filtered = ce_series_df[
    (ce_series_df["category_code"]      == "EXPEND") &
    (ce_series_df["process_code"]       == "M")      &
    (ce_series_df["demographics_code"]  == "LB01")   &
    (ce_series_df["characteristics_code"].isin(quints))
]

# ────────────────────────────────────────────────────────────────────────────────
# Load CPI (Consumer Price Index) series metadata
# ────────────────────────────────────────────────────────────────────────────────
cpi_series_df = (
    pd.read_csv(output_dir / "cu.series", sep="\t")
      .apply(lambda col: col.str.strip() if col.dtype == "object" else col)
)
cpi_series_df.columns = cpi_series_df.columns.str.strip()

# Filter CPI: U.S. City Average, not seasonally adjusted, annual average
cpi_filtered = cpi_series_df[
    # (cpi_series_df["area_code"]        == "0000") &   # U.S. city average
    # (cpi_series_df["seasonal"]         == "U")    &   # unadjusted
    (cpi_series_df["periodicity_code"] == "A")        # annual average
]

# ────────────────────────  series dict  ───────────────────

total_filtered = pd.concat([ce_filtered[["series_id","begin_year","end_year"]], cpi_filtered[["series_id","begin_year","end_year"]]])

series_dict = {
    row.series_id: {"begin": int(row.begin_year), "end": int(row.end_year)}
    for _, row in total_filtered.iterrows()
}

# ────────────────────────────────────────────────────────────────────────────────
# BLS API batch download  |  50 series × 20 years per call
# ────────────────────────────────────────────────────────────────────────────────
year_ranges = [(yr, min(yr+19, 2025)) for yr in range(1984, 2026, 20)]
batch_size  = 50

for start, end in year_ranges:

    remaining = [
        sid for sid, yrs in series_dict.items()
        if yrs["begin"] <= end and yrs["end"] >= start
        and not (requests_dir / f"{sid}_{start}_{end}.json").exists()
    ]
    if not remaining:
        continue

    batches = [remaining[i:i+batch_size] for i in range(0, len(remaining), batch_size)]

    for batch in batches:
        payload = {
            "seriesid": batch,
            "startyear": start,
            "endyear": end
        }
        if bls_api_key:
            payload["registrationkey"] = bls_api_key

        resp = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=payload, timeout=30
        )

        if resp.status_code == 200 and resp.json().get("status") == "REQUEST_SUCCEEDED":
            for s in resp.json()["Results"]["series"]:
                sid = s["seriesID"]
                out_file = requests_dir / f"{sid}_{start}_{end}.json"
                out_file.write_text(
                    json.dumps({"Results": {"series": [s]}}, indent=2),
                    encoding="utf-8"
                )
                print(f"✅ {out_file.name}")
        else:
            print(f"❌ Batch {start}-{end} HTTP {resp.status_code}")
            print(resp.text[:400])

        time.sleep(0.25)   # stay below 50 requests / 10 s
