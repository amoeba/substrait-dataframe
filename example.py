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
print("*" * 80)
print("select * from penguins")
print("*" * 80)
print(df.execute().to_pandas())


# select island, species from penguins
print("*" * 80)
print("select island, species from penguins")
print("*" * 80)
print(
    df.select([Field("island", "string"), Field("species", "string")])
    .execute()
    .to_pandas()
)

# select island from penguins
print("*" * 80)
print("select island from penguins")
print("*" * 80)
print(
    df.select(
        [
            Field("island", "string"),
        ]
    )
    .execute()
    .to_pandas()
)

# select species from penguins where island = 'Dream'
print("*" * 80)
print("select species from penguins where island = 'Dream'")
print("*" * 80)
print(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .execute()
    .to_pandas()
)

# select species from penguins where island = 'Dream' LIMIT 5
print("*" * 80)
print("select species from penguins where island = 'Dream' LIMIT 5")
print("*" * 80)
print(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .limit(5)
    .execute()
    .to_pandas()
)
