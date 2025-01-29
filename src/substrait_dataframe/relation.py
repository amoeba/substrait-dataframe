from substrait.proto import Plan, PlanRel, RelRoot, ReadRel, Rel, NamedStruct, Type


class Field:
    def __init__(self, name, type):
        self.name = name
        self.type = self.lookup_type(type)

    def lookup_type(self, type_shortname, nullable=False):
        if nullable:
            nullability = Type.Nullability.NULLABILITY_REQUIRED
        else:
            nullability = Type.Nullability.NULLABILITY_NULLABLE

        if type_shortname == "string":
            return Type(string=Type.String(nullability=nullability))
        else:
            raise Exception(f"Field type {type} not supported yet.")


class Relation:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields


def select(self, *names):
    pass


def filter(self, something):
    pass
