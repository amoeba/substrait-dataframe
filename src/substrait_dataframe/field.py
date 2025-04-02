from substrait.proto import (
    Type,
)


class Field:
    def __init__(self, name, type):
        """
        Create a new Field.

        Parameters
        ----------
        name: str
            The name of the field.
        type: str
            The type of the field. Supported types are "string", "i32", "i64",
            "fp32", and "fp64".

        Examples
        --------
        >>> from substrait_dataframe import Field, Relation

        >>> f1 = Field("species", "string")
        >>> f2 = Field("island", "string")

        >>> r1 = Relation(name="penguins", fields=[f1, f2])
        """
        self.name = name
        self.type = self.lookup_type(type)

    def __eq__(self, other):
        if self.name != other.name:
            return False

        if self.type != other.type:
            return False

        return True

    def __repr__(self):
        return f"Field({self.name})"

    def lookup_type(self, type_shortname, nullable=False):
        if nullable:
            nullability = Type.Nullability.NULLABILITY_REQUIRED
        else:
            nullability = Type.Nullability.NULLABILITY_NULLABLE

        if type_shortname == "string":
            return Type(string=Type.String(nullability=nullability))
        elif type_shortname == "i32":
            return Type(i32=Type.I32(nullability=nullability))
        elif type_shortname == "i64":
            return Type(i64=Type.I64(nullability=nullability))
        elif type_shortname == "fp32":
            return Type(fp32=Type.FP32(nullability=nullability))
        elif type_shortname == "fp64":
            return Type(fp64=Type.FP64(nullability=nullability))
        else:
            raise Exception(f"Field type {type} not supported yet.")
