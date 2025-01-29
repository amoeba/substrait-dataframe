import duckdb
from substrait.proto import Plan, PlanRel, RelRoot, ReadRel, Rel, NamedStruct, Type
from substrait.json import dump_json

from substrait_dataframe.relation import Field, Relation


class DataFrame:
    def __init__(self, relation: Relation = None):
        self.relation = relation

    def select(self, *names):
        new_relation = self.relation.select(names)

        DataFrame(relation=new_relation)

    def filter(self, something):
        new_relation = self.relation.filter(something)

        DataFrame(relation=new_relation)

    def fetch(self):
        rel = Relation(
            "penguins",
            fields=[
                Field("species", "string"),
                Field("island", "string"),
            ],
        )
        filter_fields = [Field("species", "string")]

        plan = Plan(
            relations=[
                PlanRel(
                    root=RelRoot(
                        names=[field.name for field in filter_fields],
                        input=Rel(
                            read=ReadRel(
                                named_table=ReadRel.NamedTable(names=[rel.name]),
                                base_schema=NamedStruct(
                                    names=[field.name for field in rel.fields],
                                    struct=Type.Struct(
                                        types=[field.type for field in rel.fields]
                                    ),
                                ),
                            )
                        ),
                    )
                )
            ]
        )

        serialized_plan = plan.SerializeToString()
        con = duckdb.connect()
        con.sql("INSTALL substrait;")
        con.sql("LOAD substrait;")
        # Cheating: Load the data
        con.sql("create table 'penguins' as select * from 'data/penguins.parquet';")
        # End cheating
        out_rel = con.from_substrait(proto=serialized_plan)

        print(out_rel)
