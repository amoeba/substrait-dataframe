from substrait.proto import (
    Expression,
    Plan,
    PlanRel,
    ProjectRel,
    RelRoot,
    ReadRel,
    Rel,
    NamedStruct,
    Type,
    Version,
)


class Relation:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.selected_fields = []

    def select(self, fields):
        self.selected_fields = [f for f in fields]

        return self

    # def filter(self, something):
    #     pass

    def to_substrait(self):
        # TODO: Add in producer agent
        # return Plan(
        #     relations=[
        #         PlanRel(
        #             root=RelRoot(
        #                 names=[field.name for field in self.fields],
        #                 input=Rel(
        #                     read=ReadRel(
        #                         named_table=ReadRel.NamedTable(names=[self.name]),
        #                         base_schema=NamedStruct(
        #                             names=[field.name for field in self.fields],
        #                             struct=Type.Struct(
        #                                 types=[field.type for field in self.fields]
        #                             ),
        #                         ),
        #                     )
        #                 ),
        #             )
        #         )
        #     ]
        # )
        return Plan(
            relations=[
                PlanRel(
                    root=RelRoot(
                        names=[field.name for field in self.selected_fields],
                        input=Rel(
                            project=ProjectRel(
                                input=Rel(
                                    read=ReadRel(
                                        named_table=ReadRel.NamedTable(
                                            names=[self.name]
                                        ),
                                        base_schema=NamedStruct(
                                            names=[field.name for field in self.fields],
                                            struct=Type.Struct(
                                                types=[
                                                    field.type for field in self.fields
                                                ],
                                                nullability=Type.Nullability.NULLABILITY_REQUIRED,
                                            ),
                                        ),
                                        projection=Expression.MaskExpression(
                                            select=Expression.MaskExpression.StructSelect(
                                                struct_items=[
                                                    Expression.MaskExpression.StructItem(
                                                        field=self.fields.index(field)
                                                    )
                                                    for field in self.selected_fields
                                                ]
                                            ),
                                            maintain_singular_struct=True,
                                        ),
                                    ),
                                    filter=None,
                                ),
                                expressions=[
                                    Expression(
                                        selection=Expression.FieldReference(
                                            direct_reference=Expression.ReferenceSegment(
                                                struct_field=Expression.ReferenceSegment.StructField(
                                                    field=self.selected_fields.index(
                                                        field
                                                    )
                                                )
                                            ),
                                            root_reference=Expression.FieldReference.RootReference(),
                                        )
                                    )
                                    for field in self.selected_fields
                                ],
                            )
                        ),
                    )
                )
            ],
            version=Version(producer="SubstraitDataFrame"),
        )
