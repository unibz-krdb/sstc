from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DefinitionNode,
    DependencyNode,
    FunctionalDependencyNode,
    MultivaluedDependencyNode,
    PrimaryKeyNode,
    UnaryDependencyNode,
)


class TargetTable:

    assign_node: AssignNode
    dependency_nodes: list[DependencyNode]

    def __init__(self, assign_node: AssignNode, dependency_nodes: list[DependencyNode]):
        if assign_node.name is None:
            raise ValueError("DefinitionNode must have a name")
        self.assign_node = assign_node
        self.dependency_nodes = dependency_nodes

    @property
    def name(self) -> str:
        return self.assign_node.name

    @property
    def attributes(self) -> list[str]:
        return self.assign_node.attributes.names

    @classmethod
    def from_relations_and_dependencies(
        cls,
        assign_nodes: list[AssignNode],
        dependency_nodes: list[DependencyNode],
    ) -> list["TargetTable"]:
        source_tables: list[TargetTable] = []
        for assign_node in assign_nodes:
            dependencies: list[DependencyNode] = []
            for dependency_node in dependency_nodes:
                if isinstance(
                    dependency_node,
                    UnaryDependencyNode,
                ):
                    if dependency_node.relation_name == assign_node.name:
                        dependencies.append(dependency_node)
                elif isinstance(dependency_node, BinaryDependencyNode):
                    if (
                        dependency_node.left_child.name == assign_node.name
                        or dependency_node.right_child.name == assign_node.name
                    ):
                        dependencies.append(dependency_node)
            source_tables.append(
                cls(assign_node=dependency_node, dependency_nodes=dependencies)
            )

        return source_tables
