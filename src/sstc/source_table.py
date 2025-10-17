from rapt2.treebrd.node import DefinitionNode, DependencyNode

from .table import Table


class SourceTable(Table[DefinitionNode]):
    """Source table with a DefinitionNode."""

    def __init__(
        self,
        definition_node: DefinitionNode | None = None,
        dependency_nodes: list[DependencyNode] | None = None,
        node: DefinitionNode | None = None,
    ):
        # Accept both 'definition_node' and 'node' for backward compatibility
        if definition_node is None and node is None:
            raise ValueError("Either 'definition_node' or 'node' must be provided")
        if dependency_nodes is None:
            raise ValueError("'dependency_nodes' must be provided")
        actual_node = definition_node if definition_node is not None else node
        super().__init__(node=actual_node, dependency_nodes=dependency_nodes)


    @classmethod
    def from_relations_and_dependencies(
        cls,
        definition_nodes: list[DefinitionNode],
        dependency_nodes: list[DependencyNode],
    ) -> list["SourceTable"]:
        """Create source tables from definition nodes and dependency nodes."""
        return super().from_relations_and_dependencies(
            nodes=definition_nodes, dependency_nodes=dependency_nodes
        )
