import pytest
import duckdb

from substrait_dataframe import DuckDBBackend, DataFrame, Field, Relation


@pytest.fixture
def connection():
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL substrait FROM community;")
    con.sql("LOAD substrait;")
    con.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

    return con


@pytest.fixture
def no_backend():
    return DataFrame(
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
        )
    )


@pytest.fixture
def penguins(connection):
    backend = DuckDBBackend(connection)

    return DataFrame(
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
        backend=backend,
    )
