import duckdb
from substrait.proto import Plan, PlanRel, RelRoot, ReadRel, Rel, NamedStruct, Type
from substrait.json import dump_json

from substrait_dataframe.relation import Field, Relation


class DataFrame:
    def __init__(self, relation: Relation = None):
        self.relation = relation

    def select(self, names):
        return DataFrame(relation=self.relation.select(names))

    # def filter(self, filters):
    #     new_relation = self.relation.filter(filters)

    #     DataFrame(relation=new_relation)

    def to_substrait(self):
        return self.relation.to_substrait()

    def fetch(self):
        plan = self.to_substrait()
        print(plan)
        serialized_plan = plan.SerializeToString()

        # TODO: Move this out
        con = duckdb.connect()
        con.sql("INSTALL substrait;")
        con.sql("LOAD substrait;")
        # Cheating: Load the data
        con.sql("create table 'penguins' as select * from 'data/penguins.parquet';")
        # End cheating

        out_rel = con.from_substrait(proto=serialized_plan)
        print(out_rel)
