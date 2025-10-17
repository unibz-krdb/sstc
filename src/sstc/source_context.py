from rapt2.rapt import Rapt
from rapt2.treebrd.node import DefinitionNode, Node

from .context import Context
from .source_table import SourceTable


class SourceContext(Context[SourceTable]):
    @property
    def source_tables(self) -> list[SourceTable]:
        """Alias for tables property for backward compatibility."""
        return self.tables

    @classmethod
    def _get_table_class(cls) -> type[SourceTable]:
        return SourceTable

    @classmethod
    def _is_relation_node(cls, node: Node) -> bool:
        return isinstance(node, DefinitionNode)

    @classmethod
    def from_file(cls, file_path: str):
        with open(file_path, "r") as file:
            content = file.read()
        return cls.from_string(instring=content)

    @classmethod
    def from_string(cls, instring: str):
        syntax_tree = Rapt(grammar="Dependency Grammar").to_syntax_tree(instring)
        return cls.from_syntax_tree(syntax_tree=syntax_tree)
