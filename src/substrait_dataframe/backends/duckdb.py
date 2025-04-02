import pyarrow
from substrait.proto import Plan

from substrait_dataframe.backends import Backend


class DuckDBBackend(Backend):
    def __init__(self, connection):
        self.connection = connection

    def sql(self, query_string):
        return self.connection.sql(query_string)

    def execute(self, plan: Plan) -> pyarrow.Table:
        return self.from_substrait(proto=plan.SerializeToString()).to_arrow_table()

    def get_substrait(self, sql):
        """
        Converts the provided query into a binary Substrait plan
        """

        return self.sql(f'CALL get_substrait("{sql}")')

    def get_substrait_json(self, sql):
        """
        Converts the provided query into a Substrait plan in JSON
        """

        return self.sql(f'CALL get_substrait_json("{sql}")')

    def from_substrait(self, proto):
        """
        Executes a binary Substrait plan (provided as bytes) against DuckDB and
        returns the results
        """

        blob = "".join([f"\\x{b:02x}" for b in proto])
        return self.sql(f"CALL from_substrait('{blob}'::BLOB);")

    def from_substrait_json(self, json):
        """
        Executes a Substrait plan written in JSON against DuckDB and returns the
        results
        """

        return self.sql(f"CALL from_substrait_json('{json}'::VARCHAR);")
