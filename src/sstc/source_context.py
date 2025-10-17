from rapt2.rapt import Rapt
from rapt2.treebrd.node import DefinitionNode, DependencyNode, Node
from rapt2.treebrd.schema import Schema

from .source_table import SourceTable


class SourceContext:
    source_tables: list[SourceTable]
    dependencies: list[DependencyNode]
    schema: Schema

    def __init__(self, source_tables: list[SourceTable]):
        self.source_tables = source_tables
        self.schema = Schema()
        for relation in source_tables:
            self.schema.add(relation.name, relation.attributes)

    @classmethod
    def from_file(cls, file_path: str):
        with open(file_path, "r") as file:
            content = file.read()
        return cls.from_string(instring=content)

    @classmethod
    def from_string(cls, instring: str):
        syntax_tree = Rapt(grammar="Dependency Grammar").to_syntax_tree(instring)
        return cls.from_syntax_tree(syntax_tree=syntax_tree)

    @classmethod
    def from_syntax_tree(cls, syntax_tree: list[Node]):
        relations: list[DefinitionNode] = []
        dependencies: list[DependencyNode] = []
        for node in syntax_tree:
            if isinstance(node, DefinitionNode):
                relations.append(node)
            elif isinstance(node, DependencyNode):
                dependencies.append(node)
            else:
                raise ValueError(f"Unexpected node type: {type(node)}")

        source_tables = SourceTable.from_relations_and_dependencies(
            definition_nodes=relations,
            dependency_nodes=dependencies,
        )

        return cls(source_tables=source_tables)
