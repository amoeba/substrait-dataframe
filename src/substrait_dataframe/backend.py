class Backend:
    def __init__(self, connection):
        self.connection = connection

    def sql(self, query_string):
        return self.connection.sql(query_string)

    def from_substrait(self, proto):
        # TODO: Is there a better/simpler way to do this?
        blob = "".join([f"\\x{b:02x}" for b in proto])
        return self.connection.sql(f"CALL from_substrait('{blob}'::BLOB);")


class DuckDBBackend(Backend):
    def __init__(self, connection):
        super().__init__(connection)

    def enable(self):
        self.sql("INSTALL substrait FROM community;")
        self.sql("LOAD substrait;")
        self.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

        return self
