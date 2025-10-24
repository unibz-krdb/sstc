from sstc.definition import SourceDefinition

from .table import Table


class SourceTable(Table[SourceDefinition]):
    """Source table with a DefinitionNode."""

    def create_stmt(self) -> str:
        table_name = self.name
        columns = []
        
        for attr in self.definition.schema.attributes:
            column_def = f"{attr.name} {attr.data_type}"
            if not attr.is_nullable:
                column_def += " NOT NULL"
            columns.append(column_def)
        
        columns_str = ",\n    ".join(columns)
        return f"CREATE TABLE {table_name} (\n    {columns_str}\n)"
