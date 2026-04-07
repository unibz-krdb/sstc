from typing import Self

from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DependencyNode,
    UnaryDependencyNode,
)

from .definition import AttributeSchema


class Table:
    """Generic base class for source and target tables."""

    definition: AssignNode
    dependency_nodes: list[DependencyNode]
    universal_schema: list[AttributeSchema]
    universal_mapping: AssignNode

    def __init__(
        self,
        node: AssignNode,
        dependency_nodes: list[DependencyNode],
        universal_schema: list[AttributeSchema],
        universal_mapping: AssignNode,
    ):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.definition = node
        self.dependency_nodes = dependency_nodes
        self.universal_schema = universal_schema
        self.universal_mapping = universal_mapping

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
        universal_schema: list[AttributeSchema],
        universal_mapping: AssignNode,
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
                    universal_schema=universal_schema,
                    universal_mapping=universal_mapping,
                )
            )

        return tables
