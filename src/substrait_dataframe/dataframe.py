import duckdb

from substrait_dataframe.backend import Backend
from substrait_dataframe.expression import Expression
from substrait_dataframe.relation import Relation


class DataFrame:

    def __init__(self, relation: Relation, backend: Backend = None):
        self.relation = relation
        self.backend = backend

    def select(self, names):
        self.relation = self.relation.select(names)

        return self

    def filter(self, expression: Expression):
        self.relation = self.relation.filter(expression)

        return self

    def limit(self, count: int):
        self.relation = self.relation.limit(count)

        return self

    def to_substrait(self):
        return self.relation.to_substrait()

    def plan(self):
        return self.relation.substrait_plan()

    def execute(self):
        if self.backend is None:
            raise Exception("Backend not set.")

        plan = self.to_substrait()
        serialized_plan = plan.SerializeToString()

        return self.backend.from_substrait(proto=serialized_plan).to_arrow_table()
