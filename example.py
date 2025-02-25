import duckdb

from substrait_dataframe import DuckDBBackend, DataFrame, Field, Relation
from substrait_dataframe.expression import Expression

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


# select * from penguins
print(
    df.select([Field("island", "string"), Field("species", "string")])
    .execute()
    .to_pandas()
)

# select island from penguins
print(
    df.select(
        [
            Field("island", "string"),
        ]
    )
    .execute()
    .to_pandas()
)

# select species from penguins where island = 'Dream';
print(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .execute()
    .to_pandas()
)
