import duckdb

from substrait_dataframe import Backend, DataFrame, Field, Relation

con = duckdb.connect()

df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    ),
    backend=Backend(con).enable(),
)


plan = df.select([Field("island", "string"), Field("species", "string")]).plan()
import substrait.json

# json
substrait.json.write_json(plan, "tools/plan.json")

# protobuf binary
with open("tools/plan.bin", "wb") as f:
    f.write(plan.SerializeToString())
