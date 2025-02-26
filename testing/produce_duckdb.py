import os
import json

import duckdb

from substrait_dataframe.backend import DuckDBBackend

con = duckdb.connect()
backend = DuckDBBackend(con)
backend.enable()

base_dir = os.path.join("testing", "queries")
query_files = os.listdir(base_dir)
produced_path_prefix = os.path.join("testing", "produced", "duckdb")

for query_file in query_files:
    with open(os.path.join(base_dir, query_file)) as query_reader:
        query_text = query_reader.read()

    out_path = os.path.join(produced_path_prefix, f"{query_file}.json")

    with open(out_path, "w") as f:
        f.writelines(
            json.dumps(
                json.loads(backend.get_substrait_json(query_text).fetchone()[0]),
                indent=2,
            )
        )
