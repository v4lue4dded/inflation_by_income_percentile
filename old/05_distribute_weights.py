from pathlib import Path
import json
import pandas as pd
import duckdb
import re
import copy
main_folder   = Path(__file__).resolve().parent
db_path       = main_folder / "data" / "processing" / "inflation.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)      # be sure the data/ folder exists

con = duckdb.connect(db_path)

df = pd.read_sql("select * from processing.flatfile where year = 1984 order by type_of_quintile, year, id", con=con)

con.close()



list_of_dict = copy.deepcopy(df.to_dict("records"))

current_year = 1983
current_level = -1
unallocated_expenditure = {
    0:0,
    1:0,
    2:0,
    3:0,
    4:0,
}
dicts_to_add = []

for i_dict in list_of_dict:
    print(i_dict)
    print(unallocated_expenditure)
    if current_year != i_dict["year"]:
        assert i_dict['level'] == 0, "assert i_dict['level'] == 0"
        unallocated_expenditure[0] = i_dict["cx_value"]
        current_year = i_dict["year"]
        current_level = 0
    elif current_level > i_dict["level"]:
        current_level = i_dict["level"]
        unallocated_expenditure[current_level-1 ] -= i_dict["cx_value"]
        unallocated_expenditure[i_dict["level"]] += i_dict["cx_value"]




