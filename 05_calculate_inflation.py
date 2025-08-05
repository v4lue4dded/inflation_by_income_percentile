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
df = pd.read_sql("select * from processing.flatfile order by type_of_quintile_txt, id, year", con=con)
con.close()

df.columns = df.columns.str.lower()
df = df.loc[:,['id', 'year', 'level', 'series_category', 'expenditure categories with spaces', 'use_1997', 'level_0', 'level_1', 'level_2', 'level_3', 'level_4', 'type_of_quintile_txt', 'series_id_cx', 'type_of_quintile', 'series_id_cu', 'cx_value', 'cu_value', 'is_valid_data']]

df = df.sort_values(by=['type_of_quintile_txt', 'series_id_cx', 'year'], ascending=True)\
.assign(
    type_of_quintile_txt_orderd = lambda x: x["type_of_quintile"] + '-' + x["type_of_quintile_txt"],
    cu_value_next_year = lambda x: x.groupby(['type_of_quintile_txt', 'series_id_cx'])["cu_value"].shift(-1),
    cx_value_cu_value_times = lambda x: x["cu_value"] * x["cx_value"],
    cx_value_cu_value_next_year_times = lambda x: x["cu_value_next_year"] * x["cx_value"],
)

df_agg = df[['type_of_quintile_txt_orderd', 'year','cx_value_cu_value_times','cx_value_cu_value_next_year_times', ]].groupby(['type_of_quintile_txt_orderd', 'year']).sum().reset_index()

df_agg = (
    df_agg.sort_values(["type_of_quintile_txt_orderd", "year"])
    .loc[lambda df: (df["year"] >= 1998) & (df["year"] <= 2020)]
    .assign(
        inflation_factor=lambda x: x["cx_value_cu_value_next_year_times"] / x["cx_value_cu_value_times"]
    )
    .assign(
        cumulative_inflation=lambda x: x.groupby("type_of_quintile_txt_orderd")["inflation_factor"].cumprod()
    )
)


df_agg.head()


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Wide table: index = year, columns = type, values = cumulative_inflation (mean if duplicates exist)
wide = (df_agg
        .sort_values('year')
        .pivot_table(index='year',
                     columns='type_of_quintile_txt_orderd',
                     values='cumulative_inflation',
                     aggfunc='mean'))

# (Optional) ensure every year shows up on the x-axis
years = np.arange(wide.index.min(), wide.index.max() + 1)
wide = wide.reindex(years)

ax = wide.plot(marker='o')  # one line per column (type)
ax.set_xlabel('Year')
ax.set_ylabel('cumulative_inflation')
ax.set_title('cumulative_inflation over time by quintile')
ax.legend(title='type_of_quintile_txt_orderd')  # adjust ncols as you like
ax.grid(True, alpha=0.3)
plt.show()

