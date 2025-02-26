class Backend:
    def __init__(self, connection):
        self.connection = connection

    def sql(self, query_string):
        return self.connection.sql(query_string)


class DuckDBBackend(Backend):
    def __init__(self, connection):
        super().__init__(connection)

    def enable(self):
        self.sql("INSTALL substrait FROM community;")
        self.sql("LOAD substrait;")
        self.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

        return self

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
