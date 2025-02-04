import os
import json

import duckdb

con = duckdb.connect()
con.sql("INSTALL substrait;")
con.sql("LOAD substrait;")
con.sql("CREATE TABLE 'penguins' AS SELECT * FROM '../data/penguins.parquet';")


base_dir = "./queries"
query_files = os.listdir(base_dir)
produced_path_prefix = os.path.join("produced", "duckdb")

for query_file in query_files:
    with open(os.path.join(base_dir, query_file)) as query_reader:
        query_text = query_reader.read()

    out_path = os.path.join(produced_path_prefix, f"{query_file}.json")

    with open(out_path, "w") as f:
        f.writelines(
            json.dumps(
                json.loads(con.get_substrait_json(query_text).fetchone()[0]),
                indent=2,
            )
        )
