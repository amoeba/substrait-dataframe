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


class Relation:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.selected_fields = []

    def select(self, fields):
        # TODO: Handle repeated .select() calls
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
                                                ]
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
            ]
        )
