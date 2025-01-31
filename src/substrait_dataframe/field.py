from substrait.proto import (
    Type,
)


class Field:
    def __init__(self, name, type):
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
