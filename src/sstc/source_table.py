from rapt2.treebrd.node import DefinitionNode, DependencyNode


class SourceTable:

    definition_node: DefinitionNode
    dependency_nodes: list[DependencyNode]

    def __init__(
        self, definition_node: DefinitionNode, dependency_nodes: list[DependencyNode]
    ):
        self.definition_node = definition_node
        self.dependency_nodes = dependency_nodes
