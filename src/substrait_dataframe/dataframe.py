import duckdb

from substrait_dataframe.backend import Backend
from substrait_dataframe.relation import Relation


class DataFrame:

    def __init__(self, relation: Relation, backend: Backend = None):
        self.relation = relation
        self.backend = backend

    def select(self, names):
        return DataFrame(relation=self.relation.select(names), backend=self.backend)

    # def filter(self, filters):
    #     new_relation = self.relation.filter(filters)

    #     DataFrame(relation=new_relation)

    def to_substrait(self):
        return self.relation.to_substrait()

    def execute(self):
        if self.backend is None:
            raise Exception("Backend not set.")

        plan = self.to_substrait()
        serialized_plan = plan.SerializeToString()

        return self.backend.from_substrait(proto=serialized_plan).to_arrow_table()
