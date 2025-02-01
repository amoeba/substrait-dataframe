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
    FunctionArgument,
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
        plan = Plan(
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
                                        filter=self.substrait_filter(),
                                    ),
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
            extensions=self.substrait_extensions(),
            extension_uris=self.substrait_extension_uris(),
            version=Version(producer="SubstraitDataFrame"),
        )

        return plan

    def substrait_filter(self):
        if self.current_filter is None:
            return None

        return Expression(
            scalar_function=Expression.ScalarFunction(
                function_reference=1,
                output_type=Type(
                    bool=Type.Boolean(nullability=Type.Nullability.NULLABILITY_REQUIRED)
                ),
                arguments=[
                    FunctionArgument(
                        value=Expression(
                            scalar_function=Expression.ScalarFunction(
                                function_reference=1,
                                output_type=Type(
                                    string=Type.String(
                                        nullability=Type.Nullability.NULLABILITY_REQUIRED
                                    )
                                ),
                                arguments=[
                                    FunctionArgument(
                                        value=Expression(
                                            selection=Expression.FieldReference(
                                                direct_reference=Expression.ReferenceSegment(
                                                    struct_field=Expression.ReferenceSegment.StructField(
                                                        field=1
                                                    )
                                                ),
                                                root_reference=Expression.FieldReference.RootReference(),
                                            )
                                        )
                                    ),
                                    FunctionArgument(
                                        value=Expression(
                                            literal=Expression.Literal(string="Dream")
                                        )
                                    ),
                                ],
                            )
                        )
                    ),
                    FunctionArgument(
                        value=Expression(
                            scalar_function=Expression.ScalarFunction(
                                function_reference=2,
                                output_type=Type(
                                    string=Type.String(
                                        nullability=Type.Nullability.NULLABILITY_REQUIRED
                                    )
                                ),
                                arguments=[
                                    FunctionArgument(
                                        value=Expression(
                                            selection=Expression.FieldReference(
                                                direct_reference=Expression.ReferenceSegment(
                                                    struct_field=Expression.ReferenceSegment.StructField(
                                                        field=1
                                                    )
                                                ),
                                                root_reference=Expression.FieldReference.RootReference(),
                                            )
                                        )
                                    ),
                                ],
                            )
                        )
                    ),
                ],
            )
        )

    def substrait_extensions(self):
        if self.current_filter is None:
            return []

        if type(self.current_filter) == expr.Expression.IsInStringLiteral:
            return self.current_filter.extensions()
        else:
            raise Exception("Filter type not supported")

    def substrait_extension_uris(self):
        if self.current_filter is None:
            return []

        if type(self.current_filter) == expr.Expression.IsInStringLiteral:
            return self.current_filter.extension_uris()
        else:
            raise Exception("Filter type not supported")
