from substrait_dataframe.relation import Field


def test_field_equality_inequality():
    # same name same type
    assert Field("a", "string") == Field("a", "string")
    # TODO: Not currently supported
    # same name different type
    # assert Field("a", "string") == Field("a", "string")
    # different name same type
    assert Field("a", "string") != Field("b", "string")
    # TODO: Not currently supported
    # different name different type
    # assert Field("a", "string") != Field("b", "string")


def test_indexing():
    fields = [Field("a", "string"), Field("d", "string"), Field("c", "string")]
    assert fields.index(Field("c", "string")) == 2
