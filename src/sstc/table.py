from typing import Generic, Self, TypeVar

from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DefinitionNode,
    DependencyNode,
    UnaryDependencyNode,
)

# Generic type for the node that can be either DefinitionNode or AssignNode
NodeType = TypeVar("NodeType", DefinitionNode, AssignNode)


class Table(Generic[NodeType]):
    """Generic base class for source and target tables."""

    node: NodeType
    dependency_nodes: list[DependencyNode]

    def __init__(self, node: NodeType, dependency_nodes: list[DependencyNode]):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.node = node
        self.dependency_nodes = dependency_nodes

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def attributes(self) -> list[str]:
        return self.node.attributes.names

    @classmethod
    def from_relations_and_dependencies(
        cls,
        nodes: list[NodeType],
        dependency_nodes: list[DependencyNode],
    ) -> list[Self]:
        """Create tables from a list of relation nodes and dependency nodes."""
        tables: list[Self] = []
        for node in nodes:
            dependencies: list[DependencyNode] = []
            for dependency_node in dependency_nodes:
                if isinstance(dependency_node, UnaryDependencyNode):
                    if dependency_node.relation_name == node.name:
                        dependencies.append(dependency_node)
                elif isinstance(dependency_node, BinaryDependencyNode):
                    if (
                        dependency_node.left_child.name == node.name
                        or dependency_node.right_child.name == node.name
                    ):
                        dependencies.append(dependency_node)
            tables.append(cls(node=node, dependency_nodes=dependencies))

        return tables

