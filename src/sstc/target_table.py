from rapt2.rapt import sql_translator

from .definition import TargetDefinition
from .table import Table


class TargetTable(Table[TargetDefinition]):
    """Target table with an AssignNode."""

    def create_stmt(self) -> str:
        return sql_translator.translate(
            root_list=[self.definition], use_bag_semantics=True
        )[0].replace("TEMPORARY TABLE", "TABLE")
