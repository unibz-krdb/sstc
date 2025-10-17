from rapt2.rapt import Rapt
from rapt2.treebrd.node import AssignNode, Node

from .context import Context
from .source_context import SourceContext
from .target_table import TargetTable


class TargetContext(Context[TargetTable]):
    @property
    def target_tables(self) -> list[TargetTable]:
        """Alias for tables property for backward compatibility."""
        return self.tables

    @classmethod
    def _get_table_class(cls) -> type[TargetTable]:
        return TargetTable

    @classmethod
    def _is_relation_node(cls, node: Node) -> bool:
        return isinstance(node, AssignNode)

    @classmethod
    def from_file(cls, file_path: str, source_context: SourceContext):
        with open(file_path, "r") as file:
            content = file.read()
        return cls.from_string(instring=content, source_context=source_context)

    @classmethod
    def from_string(cls, instring: str, source_context: SourceContext):
        syntax_tree = Rapt(grammar="Dependency Grammar").to_syntax_tree(
            instring, schema=source_context.schema.to_dict()
        )
        return cls.from_syntax_tree(syntax_tree=syntax_tree)
