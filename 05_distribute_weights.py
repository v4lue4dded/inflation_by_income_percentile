from pathlib import Path
import json
import pandas as pd
import duckdb
import re
main_folder   = Path(__file__).resolve().parent
db_path       = main_folder / "data" / "processing" / "inflation.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)      # be sure the data/ folder exists

con = duckdb.connect(db_path)

df = pd.read_sql("select * from processing.flatfile", con=con)

con.close()