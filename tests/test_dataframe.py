import pytest

from conftest import skip_if_missing


def test_raises_without_backend(no_backend_dataframe):
    with pytest.raises(Exception, match="Backend not set"):
        no_backend_dataframe.execute()


@skip_if_missing("duckdb")
def test_limit_validates(penguins_duckdb):
    with pytest.raises(AssertionError):
        assert penguins_duckdb.limit(0)
        assert penguins_duckdb.limit(-1)


def test_operations_dont_mutate(no_backend_dataframe):
    df = no_backend_dataframe

    # Test that calling this doesn't mutate internal state since we expect
    # it to instead return a new DataFrame
    df.limit(10)

    assert df.relation.current_limit == None
