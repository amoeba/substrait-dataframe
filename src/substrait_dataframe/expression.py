from substrait_dataframe.field import Field
from substrait_dataframe.extensions import EXTENSION, EXTENSION_URI


class Expression:
    class IsInStringLiteral:
        def __init__(self, field: Field, value: str):
            self.field = field
            self.value = value

        def extension_uris(self):
            return [EXTENSION_URI["root"], EXTENSION_URI["functions_boolean"]]

        def extensions(self):
            return [
                EXTENSION["equal:string_string"],
                EXTENSION["is_not_null:string"],
                EXTENSION["and:bool?"],
            ]
