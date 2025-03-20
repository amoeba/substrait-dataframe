import pyarrow

from substrait_dataframe import Expression, Field
from conftest import skip_if_missing


@skip_if_missing("duckdb")
def test_duckdb_dataframe_with_no_operations_fetches_all(penguins_duckdb):
    result = penguins_duckdb.execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 344


@skip_if_missing("duckdb")
def test_duckdb_filter(penguins_duckdb):
    result = penguins_duckdb.filter(
        Expression.IsInStringLiteral(Field("island", "string"), "Dream")
    ).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert len(set(result.column(1).chunk(0).to_pylist())) == 1
    assert result.num_rows == 124


@skip_if_missing("duckdb")
def test_duckdb_limit(penguins_duckdb):
    result = penguins_duckdb.limit(10).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 10


@skip_if_missing("duckdb")
def test_duckdb_select(penguins_duckdb):
    result = penguins_duckdb.select([Field("island", "string")]).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 1
    assert result.column_names[0] == "island"
    assert result.num_rows == 344


@skip_if_missing("datafusion")
def test_datafusion_dataframe_with_no_operations_fetches_all(penguins_datafusion):
    result = penguins_datafusion.execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 344


@skip_if_missing("datafusion")
def test_datafusion_filter(penguins_datafusion):
    result = penguins_datafusion.filter(
        Expression.IsInStringLiteral(Field("island", "string"), "Dream")
    ).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert len(set(result.column(1).chunk(0).to_pylist())) == 1
    assert result.num_rows == 124


@skip_if_missing("datafusion")
def test_datafusion_limit(penguins_datafusion):
    result = penguins_datafusion.limit(10).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 10


@skip_if_missing("datafusion")
def test_datafusion_select(penguins_datafusion):
    result = penguins_datafusion.select([Field("island", "string")]).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 1
    assert result.column_names[0] == "island"
    assert result.num_rows == 344
