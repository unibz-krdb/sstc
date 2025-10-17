from rapt2.treebrd.node import AssignNode, DependencyNode

from .table import Table


class TargetTable(Table[AssignNode]):
    """Target table with an AssignNode."""

    def __init__(
        self,
        assign_node: AssignNode | None = None,
        dependency_nodes: list[DependencyNode] | None = None,
        node: AssignNode | None = None,
    ):
        # Accept both 'assign_node' and 'node' for backward compatibility
        if assign_node is None and node is None:
            raise ValueError("Either 'assign_node' or 'node' must be provided")
        if dependency_nodes is None:
            raise ValueError("'dependency_nodes' must be provided")
        actual_node = assign_node if assign_node is not None else node
        super().__init__(node=actual_node, dependency_nodes=dependency_nodes)

    @classmethod
    def from_relations_and_dependencies(
        cls,
        assign_nodes: list[AssignNode],
        dependency_nodes: list[DependencyNode],
    ) -> list["TargetTable"]:
        """Create target tables from assign nodes and dependency nodes."""
        return super().from_relations_and_dependencies(
            nodes=assign_nodes, dependency_nodes=dependency_nodes
        )
