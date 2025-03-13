import duckdb

from substrait_dataframe import (
    DatafusionBackend,
    DuckDBBackend,
    DataFrame,
    Expression,
    Field,
    Relation,
)

con = duckdb.connect()
con.sql("INSTALL substrait FROM community;")
con.sql("LOAD substrait;")
con.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

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
    backend=DuckDBBackend(con),
)


# Equivalent to 'SELECT * FROM penguins'
df.execute().to_pandas()

# Equivalent to 'SELECT island, species FROM penguins'
(
    df.select([Field("island", "string"), Field("species", "string")])
    .execute()
    .to_pandas()
)

# Equivalent to 'SELECT island FROM penguins'
(
    df.select(
        [
            Field("island", "string"),
        ]
    )
    .execute()
    .to_pandas()
)

# Equivalent to 'SELECT species FROM penguins WHERE island = 'Dream'
(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .execute()
    .to_pandas()
)

# Equivalent to 'SELECT species FROM penguins WHERE island = 'Dream' LIMIT 5'
(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .limit(5)
    .execute()
    .to_pandas()
)

# Now we can switch our backend to DataFusion, run the same code, and get
# identical results.
import datafusion
import pyarrow as pa

ctx = datafusion.SessionContext(datafusion.SessionConfig())

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
    backend=DatafusionBackend(ctx),  # <-- Only this changes from before
)

# Equivalent to 'SELECT * FROM penguins'
df.execute().to_pandas()

# Equivalent to 'SELECT island, species FROM penguins'
(
    df.select([Field("island", "string"), Field("species", "string")])
    .execute()
    .to_pandas()
)

# # Equivalent to 'SELECT island FROM penguins'
# (
#     df.select(
#         [
#             Field("island", "string"),
#         ]
#     )
#     .execute()
#     .to_pandas()
# )

# # Equivalent to 'SELECT species FROM penguins WHERE island = 'Dream'
# (
#     df.select([Field("island", "string"), Field("species", "string")])
#     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
#     .execute()
#     .to_pandas()
# )

# # Equivalent to 'SELECT species FROM penguins WHERE island = 'Dream' LIMIT 5'
# (
#     df.select([Field("island", "string"), Field("species", "string")])
#     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
#     .limit(5)
#     .execute()
#     .to_pandas()
# )
