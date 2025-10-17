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

    @classmethod
    @abstractmethod
    def _get_table_class(cls) -> type[TableType]:
        """Return the table class to use for creating tables."""
        pass

    @classmethod
    @abstractmethod
    def _is_relation_node(cls, node: Node) -> bool:
        """Check if a node is a relation node (DefinitionNode or AssignNode)."""
        pass

    @classmethod
    def from_syntax_tree(cls, syntax_tree: list[Node]) -> Self:
        """Create context from a syntax tree."""
        relations = []
        dependencies: list[DependencyNode] = []
        for node in syntax_tree:
            if cls._is_relation_node(node):
                relations.append(node)
            elif isinstance(node, DependencyNode):
                dependencies.append(node)
            else:
                raise ValueError(f"Unexpected node type: {type(node)}")

        table_class = cls._get_table_class()
        tables = table_class.from_relations_and_dependencies(
            nodes=relations,
            dependency_nodes=dependencies,
        )

        return cls(tables=tables)
