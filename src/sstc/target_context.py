from typing import Self

from rapt2.rapt import Rapt
from rapt2.treebrd.node import AssignNode, DependencyNode, RelationNode

from .context import Context
from .source_context import SourceContext
from .target_table import TargetTable


class TargetContext(Context[TargetTable]):

    @classmethod
    def from_file(cls, target_path: str, source_context: SourceContext) -> Self:
        """Create context from a syntax tree."""

        with open(target_path, "r") as file:
            content = file.read()

        syntax_tree = Rapt(grammar="Dependency Grammar").to_syntax_tree(
            content, schema=source_context.schema.to_dict()
        )

        relations = []

        dependencies: list[DependencyNode] = []
        for node in syntax_tree:
            if isinstance(node, AssignNode):
                relations.append(node)
            elif isinstance(node, DependencyNode):
                dependencies.append(node)
            else:
                raise ValueError(f"Unexpected node type: {type(node)}")

        tables = TargetTable.from_relations_and_dependencies(
            definitions=relations,
            dependency_nodes=dependencies,
        )

        return cls(tables=tables)
