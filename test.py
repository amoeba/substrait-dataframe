# from zero to substrait plan
# if I only know my table name and a few columns and what filter I want
# what's the shortest bit of code I can write

from substrait.proto import Plan, PlanRel, RelRoot, ReadRel, Rel, NamedStruct, Type
from substrait.json import dump_json

table_name = "penguins"
filter_cols = ["island"]
col_names = ["species", "island"]
col_types = [
    Type(string=Type.String(nullability=Type.Nullability.NULLABILITY_REQUIRED)),
    Type(string=Type.String(nullability=Type.Nullability.NULLABILITY_REQUIRED)),
]

plan = Plan(
    relations=[
        PlanRel(
            root=RelRoot(
                names=filter_cols,
                input=Rel(
                    read=ReadRel(
                        named_table=ReadRel.NamedTable(names=[table_name]),
                        base_schema=NamedStruct(
                            names=col_names,
                            struct=Type.Struct(types=col_types),
                        ),
                    )
                ),
            )
        )
    ]
)

# Serialize
serialized_plan = plan.SerializeToString()
print(serialized_plan)

plan_json = dump_json(plan)
print(plan_json)

# pass to duckdb
import duckdb

con = duckdb.connect()
con.sql("INSTALL substrait;")
con.sql("LOAD substrait;")
# Cheating: Load the data
con.sql("create table 'penguins' as select * from 'data/penguins.parquet';")
# End cheating

print(con.from_substrait(proto=serialized_plan))
