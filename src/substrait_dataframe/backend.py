from typing import Any, Self

import datafusion
import pyarrow
from substrait.proto import Plan


class Backend:

    def __init__(self) -> Self:
        pass

    def sql(self, query: str) -> Any:
        raise NotImplementedError("Method must be implemented by subclass")

    def enabel(self) -> Any:
        raise NotImplementedError("Method must be implemented by subclass")

    def execute(self, plan: Plan) -> pyarrow.Table:
        raise NotImplementedError("Method must be implemented by subclass")


class DuckDBBackend(Backend):
    def __init__(self, connection):
        self.connection = connection

    def sql(self, query_string):
        return self.connection.sql(query_string)

    def enable(self):
        self.sql("INSTALL substrait FROM community;")
        self.sql("LOAD substrait;")
        self.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

        return self

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


class DatafusionBackend(Backend):
    def __init__(self, session):
        self.ctx = session

    def sql(self, query_string):
        # TODO
        return self.connection.sql(query_string)

    def enable(self):
        schema = pyarrow.schema(
            [
                pyarrow.field("species", pyarrow.string()),
                pyarrow.field("island", pyarrow.string()),
                pyarrow.field("bill_length_mm", pyarrow.float64()),
                pyarrow.field("bill_depth_mm", pyarrow.float64()),
                pyarrow.field("flipper_length_mm", pyarrow.int32()),
                pyarrow.field("body_mass_g", pyarrow.int32()),
                pyarrow.field("sex", pyarrow.string()),
                pyarrow.field("year", pyarrow.int32()),
            ]
        )
        self.ctx.register_parquet("penguins", "./data/penguins.parquet", schema=schema)

        return self

    def execute(self, plan: Plan) -> pyarrow.Table:
        substrait_bytes = plan.SerializeToString()
        substrait_plan = datafusion.substrait.Serde.deserialize_bytes(substrait_bytes)
        df_logical_plan = datafusion.substrait.Consumer.from_substrait_plan(
            self.ctx, substrait_plan
        )

        return self.ctx.create_dataframe_from_logical_plan(df_logical_plan)
