from typing import List, Self
from substrait.proto import (
    Plan,
    PlanRel,
    Expression,
    FetchRel,
    FilterRel,
    ProjectRel,
    RelRoot,
    ReadRel,
    Rel,
    RelCommon,
    NamedStruct,
    Type,
    Version,
)
import substrait_dataframe.expression as expr
from substrait_dataframe.field import Field


class Relation:

    def __init__(
        self, name, fields: List[Field], selection=None, filter=None, limit=None
    ) -> Self:
        self.name = name
        self.fields = fields

        self.current_selection = selection
        self.current_filter = filter
        self.current_limit = limit

    def select(self, fields: List[Field]) -> Self:
        return Relation(
            name=self.name,
            fields=self.fields,
            selection=[f for f in fields],
            filter=self.current_filter,
            limit=self.current_limit,
        )

    def filter(self, expression: expr.Expression) -> Self:
        return Relation(
            name=self.name,
            fields=self.fields,
            selection=self.current_selection,
            filter=expression,
            limit=self.current_limit,
        )

    def limit(self, count: int) -> Self:
        return Relation(
            name=self.name,
            fields=self.fields,
            selection=self.current_selection,
            filter=self.current_filter,
            limit=count,
        )
    def to_substrait(self) -> Plan:
        return Plan(
            relations=self.substrait_relations(),
            extensions=self.substrait_extensions(),
            extension_uris=self.substrait_extension_uris(),
            version=Version(
                minor_number=57, patch_number=1, producer="SubstraitDataFrame"
            ),
        )

    def substrait_relations(self) -> List[PlanRel]:
        return [PlanRel(root=self.substrait_root_rel())]

    def substrait_root_rel(self) -> RelRoot:
        # We support a few variants of plan shapes and use pattern matching to
        # choose:

        # Read (select * from foo)
        #
        # Project <- Read (select bar from foo)
        # Filter <- Read (select * from foo where baz)
        # Fetch <- Read (select * from foo limit n)
        #
        # Project <- Fetch <- Read
        # Project <- Filter <- Read
        #
        # Project <- Fetch <- Filter <- Read

        # Start with a root at the base
        root = self.wip_read()

        # If there's a filter, we add it
        if self.current_filter is not None:
            root = self.wip_filter(input=root)

        # If there's a limit, we add it
        if self.current_limit is not None:
            root = self.wip_fetch(input=root)

        # If there's a select, we add it
        if self.current_selection is not None:
            root = self.wip_project(input=root)

        # # TODO: Can remove this I think
        # # # Handle no selection
        # if self.current_selection is None:
        #     self.current_selection = self.fields

        # # To decide on the overal Plan structure, we essentially are just using
        # # pattern matching which isn't very flexible and, as you can see, we
        # # only support two very simple patterns at the moment.
        # if self.current_limit is not None:
        #     return self.substrait_root_fetch(input=self.wip_read())
        # else:
        #     return self.substrait_root_read()

        if self.current_selection is None:
            self.current_selection = self.fields

        root_names = [field.name for field in self.current_selection]

        return RelRoot(names=root_names, input=root)

    def substrait_root_fetch(self, input) -> RelRoot:
        return RelRoot(
            names=[field.name for field in self.current_selection],
            input=self.wip_fetch(input),
        )

    def substrait_root_read(self) -> RelRoot:
        return RelRoot(
            names=[field.name for field in self.current_selection],
            input=self.wip_read(),
        )

    def wip_fetch(self, input: Rel) -> Rel:
        return Rel(
            fetch=FetchRel(
                input=input,
                offset=0,
                count=self.current_limit,
            )
        )

    # This is always a terminal node
    def wip_read(self) -> Rel:
        return Rel(
            read=ReadRel(
                named_table=ReadRel.NamedTable(names=[self.name]),
                base_schema=NamedStruct(
                    names=[field.name for field in self.fields],
                    struct=Type.Struct(
                        types=[field.type for field in self.fields],
                        nullability=Type.Nullability.NULLABILITY_REQUIRED,
                    ),
                ),
            ),
        )

    def wip_filter(self, input: Rel) -> Rel:
        return Rel(filter=FilterRel())

    def wip_project(self, input: Rel) -> Rel:
        return Rel(
            project=ProjectRel(
                common=RelCommon(
                    direct=RelCommon.Direct(),
                    emit=RelCommon.Emit(
                        output_mapping=[
                            self.fields.index(field) for field in self.current_selection
                        ]
                    ),
                ),
                input=input,
                expressions=[
                    Expression(
                        selection=Expression.FieldReference(
                            direct_reference=Expression.ReferenceSegment(
                                struct_field=Expression.ReferenceSegment.StructField(
                                    field=self.fields.index(field)
                                )
                            ),
                            root_reference=Expression.FieldReference.RootReference(),
                        )
                    )
                    for field in self.current_selection
                ],
            )
        )

    def substrait_filter(self):
        if self.current_filter is None:
            return None

        return self.current_filter.to_substrait(self.fields)

    def substrait_project(self):
        if self.current_selection is None:
            return None

        return Expression.MaskExpression(
            select=Expression.MaskExpression.StructSelect(
                struct_items=[
                    Expression.MaskExpression.StructItem(field=self.fields.index(field))
                    for field in self.current_selection
                ],
            )
        )

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
