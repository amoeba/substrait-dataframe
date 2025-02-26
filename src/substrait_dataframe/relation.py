from substrait.proto import (
    Plan,
    PlanRel,
    RelRoot,
    ReadRel,
    Rel,
    NamedStruct,
    Type,
    Version,
)
import substrait_dataframe.expression as expr


class Relation:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.selected_fields = []
        self.current_filter = None

    def select(self, fields):
        self.selected_fields = [f for f in fields]

        return self

    def filter(self, expression):
        self.current_filter = expression

        return self

    def to_substrait(self):
        plan = self.substrait_plan()

        return plan

    def substrait_plan(self):
        return Plan(
            relations=self.substrait_relations(),
            extensions=self.substrait_extensions(),
            extension_uris=self.substrait_extension_uris(),
            version=Version(producer="SubstraitDataFrame"),
        )

    def substrait_relations(self):
        return [PlanRel(root=self.substrait_root_rel())]

    def substrait_root_rel(self):
        # Handle no selection
        if len(self.selected_fields) <= 0:
            self.selected_fields = self.fields

        return RelRoot(
            names=[field.name for field in self.selected_fields],
            input=Rel(
                read=ReadRel(
                    named_table=ReadRel.NamedTable(names=[self.name]),
                    base_schema=NamedStruct(
                        names=[field.name for field in self.fields],
                        struct=Type.Struct(
                            types=[field.type for field in self.fields],
                            nullability=Type.Nullability.NULLABILITY_REQUIRED,
                        ),
                    ),
                    filter=self.substrait_filter(),
                ),
            ),
        )

    def substrait_filter(self):
        if self.current_filter is None:
            return None

        return self.current_filter.to_substrait(self.fields)

    def substrait_extensions(self):
        if self.current_filter is None:
            return []

        if type(self.current_filter) == expr.Expression.IsInStringLiteral:
            return self.current_filter.substrait_extensions()
        else:
            raise Exception("Filter type not supported")

    def substrait_extension_uris(self):
        if self.current_filter is None:
            return []

        if type(self.current_filter) == expr.Expression.IsInStringLiteral:
            return self.current_filter.substrait_extension_uris()
        else:
            raise Exception("Filter type not supported")
