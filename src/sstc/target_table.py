from rapt2.treebrd.node import AssignNode

from .table import Table


class TargetTable(Table[AssignNode]):
    """Target table with an AssignNode."""

    def create_stmt(self) -> str:
        return super().create_stmt().replace("TEMPORARY TABLE", "TABLE")