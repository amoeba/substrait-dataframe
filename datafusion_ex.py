import datafusion
import pyarrow as pa
import substrait.json
import substrait.proto

# register context with data
ctx = datafusion.SessionContext()
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

# serialize
query_text = "select species from penguins where island = 'Dream';"
datafusion.substrait.Serde.serialize(query_text, ctx, "query.substrait")

# deserialize
logical_in = datafusion.substrait.Consumer.from_substrait_plan(
    ctx, datafusion.substrait.Serde.deserialize("query.substrait")
)

# check
ctx.create_dataframe_from_logical_plan(logical_in)


# json
from substrait.proto import Plan

p = Plan()
p.ParseFromString(datafusion.substrait.Serde.deserialize("query.substrait").encode())
substrait.json.write_json(p, "query.json")
