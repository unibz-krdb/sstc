from abc import ABC, abstractmethod
from typing import Generic, Self, TypeVar

from rapt2.treebrd.node import DependencyNode, Node
from rapt2.treebrd.schema import Schema

from .table import Table

# Generic type for the table class (SourceTable or TargetTable)
TableType = TypeVar("TableType", bound=Table)


class Context(ABC, Generic[TableType]):
    """Generic base class for source and target contexts."""

    tables: list[TableType]
    schema: Schema

    def __init__(self, tables: list[TableType]):
        self.tables = tables
        self.schema = Schema()
        for table in tables:
            self.schema.add(table.name, table.attributes)
