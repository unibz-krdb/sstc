from typing import Self

from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DependencyNode,
    UnaryDependencyNode,
)


class Table:
    """Wraps an AssignNode with its associated dependency nodes."""

    definition: AssignNode
    dependency_nodes: list[DependencyNode]

    def __init__(
        self,
        node: AssignNode,
        dependency_nodes: list[DependencyNode],
    ):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.definition = node
        self.dependency_nodes = dependency_nodes

    @property
    def name(self) -> str:
        if self.definition.name is None:
            raise ValueError("Node must have a name")
        return self.definition.name

    @property
    def attributes(self) -> list[str]:
        return self.definition.attributes.names

    @classmethod
    def from_relations_and_dependencies(
        cls,
        definitions: list[AssignNode],
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
            tables.append(
                cls(
                    node=definition,
                    dependency_nodes=dependencies,
                )
            )

        return tables
