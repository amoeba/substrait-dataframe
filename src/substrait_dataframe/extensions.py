from substrait.proto import SimpleExtensionDeclaration, SimpleExtensionURI

EXTENSION_URI = {
    "root": SimpleExtensionURI(
        extension_uri_anchor=1,
        uri="https://github.com/substrait-io/substrait/blob/main/extensions/",
    ),
    "functions_boolean": SimpleExtensionURI(
        extension_uri_anchor=2,
        uri="https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml",
    ),
}

EXTENSION = {
    "equal:string_string": SimpleExtensionDeclaration(
        extension_function=SimpleExtensionDeclaration.ExtensionFunction(
            extension_uri_reference=EXTENSION_URI["root"].extension_uri_anchor,
            function_anchor=1,
            name="equal:string_string",
        )
    ),
    "is_not_null:string": SimpleExtensionDeclaration(
        extension_function=SimpleExtensionDeclaration.ExtensionFunction(
            extension_uri_reference=EXTENSION_URI["root"].extension_uri_anchor,
            function_anchor=2,
            name="is_not_null:string",
        )
    ),
    "and:bool?": SimpleExtensionDeclaration(
        extension_function=SimpleExtensionDeclaration.ExtensionFunction(
            extension_uri_reference=EXTENSION_URI[
                "functions_boolean"
            ].extension_uri_anchor,
            function_anchor=3,
            name="and:bool?",
        )
    ),
}
