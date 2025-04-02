from typing import Any, Self

import pyarrow
from substrait.proto import Plan


class Backend:
    def __init__(self) -> Self:
        pass

    def sql(self, query: str) -> Any:
        raise NotImplementedError("Method must be implemented by subclass")

    def execute(self, plan: Plan) -> pyarrow.Table:
        raise NotImplementedError("Method must be implemented by subclass")
