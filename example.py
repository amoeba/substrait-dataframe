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
    backend=DuckDBBackend(con).enable(),
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

ctx = datafusion.SessionContext(datafusion.SessionConfig())

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
    backend=DatafusionBackend(ctx).enable(),  # <-- Only this changes from before
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
