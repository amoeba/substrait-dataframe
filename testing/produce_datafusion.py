import os
import json

import datafusion
import pyarrow as pa
import substrait.json
from substrait.proto import Plan

ctx = datafusion.SessionContext()
# DataFusion will convert to dictionary-encoded which makes our job harder
schema = pa.schema(
    [
        pa.field("species", pa.string()),
        pa.field("island", pa.string()),
        pa.field("bill_length_mm", pa.int32()),
        pa.field("bill_depth_mm", pa.int32()),
        pa.field("body_mass_g", pa.int32()),
        pa.field("sex", pa.string()),
        pa.field("year", pa.int32()),
    ]
)
ctx.register_parquet("penguins", "./data/penguins.parquet", schema=schema)


base_dir = os.path.join("testing", "./queries")
query_files = os.listdir(base_dir)
produced_path_prefix = os.path.join("testing", "produced", "datafusion")

for query_file in query_files:
    with open(os.path.join(base_dir, query_file)) as query_reader:
        query_text = query_reader.read()
        out_path = os.path.join(produced_path_prefix, f"{query_file}.json")

        # Serialize plan to protobuf binary
        plan_bytes = datafusion.substrait.Serde.serialize_bytes(query_text, ctx)

        # Read it in and write it out as JSON
        p = Plan()
        p.ParseFromString(plan_bytes)
        substrait.json.write_json(p, out_path)
