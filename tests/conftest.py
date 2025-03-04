import pytest
import duckdb

from substrait_dataframe import DuckDBBackend, DataFrame, Field, Relation


@pytest.fixture
def connection():
    import duckdb

    return duckdb.connect()


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
def penguins():
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
        backend=DuckDBBackend(duckdb.connect()).enable(),
    )
