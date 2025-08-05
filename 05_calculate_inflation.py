from pathlib import Path
import json
import pandas as pd
import duckdb
import re
import copy


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


main_folder   = Path(__file__).resolve().parent
db_path       = main_folder / "data" / "processing" / "inflation.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)      # be sure the data/ folder exists

con = duckdb.connect(db_path)
df_inflation = pd.read_sql("""
                select * from processing.flatfile 
                where type_of_quintile_txt not in ('Incomplete income reports','Total complete income reporters')
                order by type_of_quintile_txt, id, year
                """, con=con)

df_income = pd.read_sql("""
                select 
                    ba.year
                  , ba.type_of_quintile
                  , ba.type_of_quintile_txt
                  , cx.value as income_after_taxes
                from      processing.basis   ba
                left join main.series_import cx on ba.series_id_cx = cx.seriesID and ba.year = cx.period and cx.periodName = 'Annual'
                where ba.id = 111
                 """, con=con)

df_income = df_income.assign(
    type_of_quintile_txt_ordered = lambda x: x["type_of_quintile"] + '-' + x["type_of_quintile_txt"],
)

con.close()

df_inflation.columns = df_inflation.columns.str.lower()
df_inflation = df_inflation.loc[:,['id', 'year', 'level', 'series_category', 'expenditure categories with spaces', 'use_1997', 'level_0', 'level_1', 'level_2', 'level_3', 'level_4', 'type_of_quintile_txt', 'series_id_cx', 'type_of_quintile', 'series_id_cu', 'cx_value', 'cu_value', 'is_valid_data']]

df_inflation = df_inflation.sort_values(by=['type_of_quintile_txt', 'series_id_cx', 'year'], ascending=True)\
.assign(
    type_of_quintile_txt_ordered = lambda x: x["type_of_quintile"] + '-' + x["type_of_quintile_txt"],
    cu_value_next_year = lambda x: x.groupby(['type_of_quintile_txt', 'series_id_cx'])["cu_value"].shift(-1),
    cx_value_cu_value_times = lambda x: x["cu_value"] * x["cx_value"],
    cx_value_cu_value_next_year_times = lambda x: x["cu_value_next_year"] * x["cx_value"],
)

df_inflation_agg = df_inflation[
    ['type_of_quintile_txt_ordered'
    , 'year'
    ,'cx_value_cu_value_times'
    ,'cx_value_cu_value_next_year_times'
    , ]].groupby(
        ['type_of_quintile_txt_ordered'
        , 'year']).sum().reset_index()

df_combined = df_inflation_agg.merge(
    df_income,
    how="left",
    left_on=["year", "type_of_quintile_txt_ordered"],
    right_on=["year", "type_of_quintile_txt_ordered"],
    validate="1:1"
)

df_combined = df_combined.sort_values(
    [
        "type_of_quintile_txt_ordered"
        , "year"
    ]).assign(
        inflation_factor=lambda x: x["cx_value_cu_value_next_year_times"] / x["cx_value_cu_value_times"],
        income_after_taxes_next_year = lambda x: x.groupby(['type_of_quintile_txt'])["income_after_taxes"].shift(-1),
        income_after_taxes_factor=lambda x: x["income_after_taxes_next_year"] / x["income_after_taxes"],
        purchasing_power_factor = lambda x: x["income_after_taxes_factor"]/x["inflation_factor"]
    ).loc[
        lambda df_inflation: (df_inflation["year"] >= 1998) & (df_inflation["year"] <= 2020),:
    ].assign(
        cumulative_inflation=lambda x: x.groupby("type_of_quintile_txt_ordered")["inflation_factor"].cumprod(),
        cumulative_income=lambda x: x.groupby("type_of_quintile_txt_ordered")["income_after_taxes_factor"].cumprod(),
        cumulative_purchasing_power=lambda x: x.groupby("type_of_quintile_txt_ordered")["purchasing_power_factor"].cumprod(),
    )


df_combined.head()

import matplotlib.pyplot as plt
import numpy as np

# Wide table: index = year, columns = type, values = cumulative_inflation
wide = (df_combined
        .sort_values('year')
        .pivot_table(index='year',
                     columns='type_of_quintile_txt_ordered',
                     values='cumulative_inflation',
                     aggfunc='mean'))

years = np.arange(wide.index.min(), wide.index.max() + 1)
wide = wide.reindex(years)

fig, ax = plt.subplots(figsize=(10, 6), constrained_layout=True)

wide.plot(marker='o', linewidth=2, markersize=3, ax=ax)

ax.set_xlabel('Year')
ax.set_ylabel('Cumulative inflation')
ax.set_title('Cumulative inflation over time by quintile')
ax.grid(True, alpha=0.3)

# ⬅️ Legend outside on the right
ax.legend(
    title='type_of_quintile_txt_ordered',
    loc='center left',
    bbox_to_anchor=(1.02, 0.5),
    frameon=True,
    borderaxespad=0.0
)

plt.show()
# If saving to file, use tight bbox so the legend isn't cut off:
fig.savefig("cumulative_inflation.png", dpi=300, bbox_inches="tight")