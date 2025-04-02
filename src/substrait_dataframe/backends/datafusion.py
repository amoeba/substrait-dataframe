import datafusion
import pyarrow
from substrait.proto import Plan

from substrait_dataframe.backends import Backend


class DatafusionBackend(Backend):
    def __init__(self, session):
        self.ctx = session

    def sql(self, query_string):
        # TODO: This code doesn't work. Implement it so it does.
        return self.connection.sql(query_string)

    def execute(self, plan: Plan) -> pyarrow.Table:
        substrait_bytes = plan.SerializeToString()
        substrait_plan = datafusion.substrait.Serde.deserialize_bytes(substrait_bytes)
        df_logical_plan = datafusion.substrait.Consumer.from_substrait_plan(
            self.ctx, substrait_plan
        )

        return self.ctx.create_dataframe_from_logical_plan(
            df_logical_plan
        ).to_arrow_table()
