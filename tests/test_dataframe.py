import pytest

from substrait_dataframe import Expression, Field


def test_raises_without_backend(no_backend):
    with pytest.raises(Exception, match="Backend not set"):
        no_backend.execute()


def test_limit_validates(penguins):
    with pytest.raises(AssertionError):
        assert penguins.limit(0)
        assert penguins.limit(-1)
