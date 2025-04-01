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
        """
        Construct our root relation by building up from the bottom. Every plan
        starts with a ReadRel and we iteratively wrap that Relation in others
        as appropriate.
        """
        root = self.substrait_read()

        if self.current_filter is not None:
            root = self.substrait_filter(input=root)

        if self.current_limit is not None:
            root = self.substrait_fetch(input=root)

        # If there's a select, we add a projection, otherwise just select
        # everything without using a ProjectRel
        if self.current_selection is not None:
            root = self.substrait_project(input=root)
        else:
            self.current_selection = self.fields

        root_names = [field.name for field in self.current_selection]

        return RelRoot(names=root_names, input=root)

    def substrait_read(self) -> Rel:
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

    def substrait_filter(self, input: Rel) -> Rel:
        return Rel(
            filter=FilterRel(
                input=input, condition=self.current_filter.to_substrait(self.fields)
            )
        )

    def substrait_fetch(self, input: Rel) -> Rel:
        return Rel(
            fetch=FetchRel(
                input=input,
                offset=0,
                count=self.current_limit,
            )
        )

    def substrait_project(self, input: Rel) -> Rel:
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
