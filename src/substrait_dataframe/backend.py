class Backend:
    def __init__(self, connection):
        self.connection = connection

    def sql(self, query_string):
        return self.connection.sql(query_string)

    def from_substrait(self, proto):
        return self.connection.from_substrait(proto)


class DuckDBBackend(Backend):
    def __init__(self, connection):
        super().__init__(connection)

    def enable(self):
        self.sql("INSTALL substrait FROM community;")
        self.sql("LOAD substrait;")
        self.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

        return self
