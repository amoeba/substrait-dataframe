import importlib
import pytest

from substrait_dataframe import (
    DuckDBBackend,
    DatafusionBackend,
    DataFrame,
    Field,
    Relation,
)


def skip_if_missing(name: str):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        return pytest.mark.skip(f"{name} missing")
    return lambda func: func


@pytest.fixture
def duckdb_connection():
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL substrait FROM community;")
    con.sql("LOAD substrait;")
    con.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

    return con


@pytest.fixture
def datafusion_connection():
    import pyarrow as pa
    import datafusion

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

    return ctx


@pytest.fixture
def no_backend_dataframe():
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
def penguins_duckdb(duckdb_connection):
    backend = DuckDBBackend(duckdb_connection)

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


@pytest.fixture
def penguins_datafusion(datafusion_connection):
    backend = DatafusionBackend(datafusion_connection)

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
