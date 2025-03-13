import os

import datafusion
import pyarrow as pa
import substrait.json

ctx = datafusion.SessionContext()

schema = pa.schema(
    [
        pa.field("species", pa.string()),
        pa.field("island", pa.string()),
        pa.field("bill_length_mm", pa.float64()),
        pa.field("bill_depth_mm", pa.float64()),
        pa.field("flipper_length_mm", pa.int32()),
        pa.field("body_mass_g", pa.int32()),
        pa.field("sex", pa.string()),
        pa.field("year", pa.int32()),
    ]
)

ctx.register_parquet("penguins", "./data/penguins.parquet", schema=schema)

produced_files_path = os.path.join("testing", "produced", "duckdb")
produced_files = os.listdir(produced_files_path)

for produced_file in produced_files:
    print(f"Query: {produced_file}")

    substrait_plan = substrait.json.load_json(
        os.path.join(produced_files_path, produced_file)
    )
    substrait_bytes = substrait_plan.SerializeToString()
    substrait_plan = datafusion.substrait.Serde.deserialize_bytes(substrait_bytes)
    df_logical_plan = datafusion.substrait.Consumer.from_substrait_plan(
        ctx, substrait_plan
    )

    print("Result:")
    print(ctx.create_dataframe_from_logical_plan(df_logical_plan))
