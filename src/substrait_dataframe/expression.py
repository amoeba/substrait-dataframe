from substrait.proto import (
    Expression as SubstraitExpression,
    Type,
    FunctionArgument,
)

from substrait_dataframe.field import Field
from substrait_dataframe.extensions import EXTENSION, EXTENSION_URI


class Expression:
    class IsInStringLiteral:
        def __init__(self, field: Field, value: str):
            self.field = field
            self.value = value

        def to_substrait(self, fields):
            return SubstraitExpression(
                scalar_function=SubstraitExpression.ScalarFunction(
                    function_reference=EXTENSION[
                        "and:bool?"
                    ].extension_function.function_anchor,
                    output_type=Type(
                        bool=Type.Boolean(
                            nullability=Type.Nullability.NULLABILITY_REQUIRED
                        )
                    ),
                    arguments=[
                        FunctionArgument(
                            value=SubstraitExpression(
                                scalar_function=SubstraitExpression.ScalarFunction(
                                    function_reference=EXTENSION[
                                        "equal:string_string"
                                    ].extension_function.function_anchor,
                                    output_type=Type(
                                        string=Type.String(
                                            nullability=Type.Nullability.NULLABILITY_REQUIRED
                                        )
                                    ),
                                    arguments=[
                                        FunctionArgument(
                                            value=SubstraitExpression(
                                                selection=SubstraitExpression.FieldReference(
                                                    direct_reference=SubstraitExpression.ReferenceSegment(
                                                        struct_field=SubstraitExpression.ReferenceSegment.StructField(
                                                            field=fields.index(
                                                                self.field
                                                            )
                                                        )
                                                    ),
                                                    root_reference=SubstraitExpression.FieldReference.RootReference(),
                                                )
                                            )
                                        ),
                                        FunctionArgument(
                                            value=SubstraitExpression(
                                                literal=SubstraitExpression.Literal(
                                                    string=self.value
                                                )
                                            )
                                        ),
                                    ],
                                )
                            )
                        ),
                        FunctionArgument(
                            value=SubstraitExpression(
                                scalar_function=SubstraitExpression.ScalarFunction(
                                    function_reference=EXTENSION[
                                        "is_not_null:string"
                                    ].extension_function.function_anchor,
                                    output_type=Type(
                                        string=Type.String(
                                            nullability=Type.Nullability.NULLABILITY_REQUIRED
                                        )
                                    ),
                                    arguments=[
                                        FunctionArgument(
                                            value=SubstraitExpression(
                                                selection=SubstraitExpression.FieldReference(
                                                    direct_reference=SubstraitExpression.ReferenceSegment(
                                                        struct_field=SubstraitExpression.ReferenceSegment.StructField(
                                                            field=fields.index(
                                                                self.field
                                                            )
                                                        )
                                                    ),
                                                    root_reference=SubstraitExpression.FieldReference.RootReference(),
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

        def substrait_extension_uris(self):
            return [EXTENSION_URI["root"], EXTENSION_URI["functions_boolean"]]

        def substrait_extensions(self):
            return [
                EXTENSION["equal:string_string"],
                EXTENSION["is_not_null:string"],
                EXTENSION["and:bool?"],
            ]
