import pyarrow

from substrait_dataframe import Expression, Field


def test_dataframe_with_no_operations_fetches_all(penguins):
    result = penguins.execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 344


def test_filter(penguins):
    result = penguins.filter(
        Expression.IsInStringLiteral(Field("island", "string"), "Dream")
    ).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert len(set(result.column(1).chunk(0).to_pylist())) == 1
    assert result.num_rows == 124


def test_limit(penguins):
    result = penguins.limit(10).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 8
    assert result.num_rows == 10


def test_select(penguins):
    result = penguins.select([Field("island", "string")]).execute()

    assert type(result) == pyarrow.Table
    assert len(result.columns) == 1
    assert result.column_names[0] == "island"
    assert result.num_rows == 344
