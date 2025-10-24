from abc import abstractmethod
from typing import Generic, Self, TypeVar

from rapt2.rapt import sql_translator
from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DependencyNode,
    UnaryDependencyNode,
)
from .definition import Definition

# Generic type for the node that can be either DefinitionNode or AssignNode
DefinitionType = TypeVar("DefinitionType", bound=Definition)


class Table(Generic[DefinitionType]):
    """Generic base class for source and target tables."""

    definition: DefinitionType
    dependency_nodes: list[DependencyNode]

    def __init__(self, node: DefinitionType, dependency_nodes: list[DependencyNode]):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.definition = node
        self.dependency_nodes = dependency_nodes

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def attributes(self) -> list[str]:
        return self.definition.attributes

    @classmethod
    def from_relations_and_dependencies(
        cls,
        definitions: list[DefinitionType],
        dependency_nodes: list[DependencyNode],
    ) -> list[Self]:
        """Create tables from a list of relation nodes and dependency nodes."""
        tables: list[Self] = []
        for definition in definitions:
            dependencies: list[DependencyNode] = []
            for dependency_node in dependency_nodes:
                if isinstance(dependency_node, UnaryDependencyNode):
                    if dependency_node.relation_name == definition.name:
                        dependencies.append(dependency_node)
                elif isinstance(dependency_node, BinaryDependencyNode):
                    if (
                        dependency_node.left_child.name == definition.name
                        or dependency_node.right_child.name == definition.name
                    ):
                        dependencies.append(dependency_node)
            tables.append(cls(node=definition, dependency_nodes=dependencies))

        return tables

    @abstractmethod
    def create_stmt(self) -> str:
        raise NotImplementedError
