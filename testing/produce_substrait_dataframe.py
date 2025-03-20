import os

import datafusion
import pyarrow as pa
import substrait.json
from substrait.proto import Plan
import substrait.json

from substrait_dataframe import DataFrame, Relation, Field, Expression

df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)

produced_path_prefix = os.path.join("testing", "produced", "substrait_dataframe")

df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)
substrait.json.write_json(
    df.to_substrait(),
    os.path.join(produced_path_prefix, "select-star-from-penguins.json"),
)
df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)
substrait.json.write_json(
    df.limit(10).to_substrait(),
    os.path.join(produced_path_prefix, "select-star-from-penguins-limit-10.json"),
)
df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)
substrait.json.write_json(
    df.select([Field("species", "string")]).to_substrait(),
    os.path.join(produced_path_prefix, "select-species-from-penguins.json"),
)
df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)
substrait.json.write_json(
    df.select([Field("island", "string"), Field("species", "string")])
    .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
    .to_substrait(),
    os.path.join(
        produced_path_prefix,
        "select-species-from-penguins-where-island-equals-dream.json",
    ),
)
df = DataFrame(
    relation=Relation(
        name="penguins",
        fields=[
            Field("species", "string"),
            Field("island", "string"),
            Field("bill_length_mm", "fp64"),
            Field("bill_depth_mm", "fp64"),
            Field("flipper_length_mm", "i32"),
            Field("body_mass_g", "i32"),
            Field("sex", "string"),
            Field("year", "i32"),
        ],
    )
)
substrait.json.write_json(
    df.select([Field("island", "string"), Field("species", "string")]).to_substrait(),
    os.path.join(produced_path_prefix, "select-island-species-from-penguins.json"),
)
