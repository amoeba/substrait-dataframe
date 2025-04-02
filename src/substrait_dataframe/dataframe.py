from typing import List, Self

import pyarrow
from substrait.proto import Plan

from substrait_dataframe.backends import Backend
from substrait_dataframe.expression import Expression
from substrait_dataframe.field import Field
from substrait_dataframe.relation import Relation


class DataFrame:

    def __init__(self, relation: Relation, backend: Backend = None) -> Self:
        """
        Create a DataFrame for the given Relation in the given Backend.

        Parameters
        -----------
        relation: Relation
            A specification of a target Relation that should exist in the
            Backend.

        backend: Backend
            The subclass of Backend where relation exists.

        Examples:
        ---------
        >>> import duckdb
        >>> from substrait_dataframe import DuckDBBackend, DataFrame, Field, Relation

        >>> con = duckdb.connect()

        >>> df = DataFrame(
        ...     relation=Relation(
        ...         name="penguins",
        ...         fields=[
        ...             Field("species", "string"),
        ...             Field("island", "string"),
        ...             Field("bill_length_mm", "fp64"),
        ...             Field("bill_depth_mm", "fp64"),
        ...             Field("flipper_length_mm", "i32"),
        ...             Field("body_mass_g", "i32"),
        ...             Field("sex", "string"),
        ...             Field("year", "i32"),
        ...         ],
        ...     ),
        ...     backend=DuckDBBackend(con)
        ... )
        """

        self.relation = relation
        self.backend = backend

    def filter(self, expression: Expression) -> Self:
        """
        Filter the rows of a DataFrame.

        Note: Only one filter expression is supported at a time.

        Parameters
        -----------
        expression: Expression
                    An Expression to filter by.

        Returns
        -------
        DataFrame
            A modified version of the input DataFrame.

        Examples:
        ---------
        An Expression can be constructed like so:
        >>> expr = Expression.IsInStringLiteral(Field("island", "string"), "Dream")
        """

        return DataFrame(
            relation=self.relation.filter(expression), backend=self.backend
        )

    def limit(self, count: int) -> Self:
        """
        Limit the number of records returned.

        Parameters
        -----------
        count: int
               Number of rows to return. Must be positive.

        Returns
        -------
        DataFrame
            A modified version of the input DataFrame.

        Examples:
        ---------
        df.limit(10)
        """

        assert count > 0

        return DataFrame(relation=self.relation.limit(count), backend=self.backend)

    def select(self, fields: List[Field]) -> Self:
        """
        Select one or more columns.

        Note: Only one selection is supported at a time.

        Parameters
        -----------
        names: list of Field

        Returns
        -------
        DataFrame
            A modified version of the input DataFrame.

        Examples:
        ---------
        df.select([Field("island", "string"])
        """

        return DataFrame(relation=self.relation.select(fields), backend=self.backend)

    def execute(self) -> pyarrow.Table:
        """
        Execute the current DataFrame against the backend.

        Returns
        -------
        pyarrow.Table
            A Table with the result.

        Examples:
        ---------
        df.execute()
        """

        if self.backend is None:
            raise Exception("Backend not set.")

        plan = self.to_substrait()

        return self.backend.execute(plan)

    def to_substrait(self) -> Plan:
        """
        Convert the current DataFrame to Substrait Plan object.

        Returns
        -------
        Plan
            A Plan object representing the current DataFrame.

        Examples:
        ---------
        df.to_substrait()
        """

        return self.relation.to_substrait()
