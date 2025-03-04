from typing import List, Self
from substrait.proto import (
    Plan,
    PlanRel,
    FetchRel,
    RelRoot,
    ReadRel,
    Rel,
    NamedStruct,
    Type,
    Version,
)
import substrait_dataframe.expression as expr
from substrait_dataframe.field import Field


class Relation:

    def __init__(self, name, fields: List[Field]) -> Self:
        self.name = name
        self.fields = fields

        self.current_selection = None
        self.current_filter = None
        self.current_limit = None

    def select(self, fields: List[Field]) -> Self:
        self.current_selection = [f for f in fields]

        return self

    def filter(self, expression: expr.Expression) -> Self:
        self.current_filter = expression

        return self

    def limit(self, count: int) -> Self:
        self.current_limit = count

        return self

    def to_substrait(self) -> Plan:
        return Plan(
            relations=self.substrait_relations(),
            extensions=self.substrait_extensions(),
            extension_uris=self.substrait_extension_uris(),
            version=Version(producer="SubstraitDataFrame"),
        )

    def substrait_relations(self) -> List[PlanRel]:
        return [PlanRel(root=self.substrait_root_rel())]

    def substrait_root_rel(self) -> RelRoot:
        # Handle no selection
        if self.current_selection is None:
            self.current_selection = self.fields

        # To decide on the overal Plan structure, we essentially are just using
        # pattern matching which isn't very flexible and, as you can see, we
        # only support two very simple patterns at the moment.
        if self.current_limit is not None:
            return self.substrait_root_fetch()
        else:
            return self.substrait_root_read()

    def substrait_root_fetch(self) -> RelRoot:
        return RelRoot(
            names=[field.name for field in self.current_selection],
            input=Rel(
                fetch=FetchRel(
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
                    offset=0,
                    count=self.current_limit,
                )
            ),
        )

    def substrait_root_read(self) -> RelRoot:
        return RelRoot(
            names=[field.name for field in self.current_selection],
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
