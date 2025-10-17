from rapt2.rapt import Rapt
from rapt2.treebrd.node import AssignNode, DependencyNode, Node
from rapt2.treebrd.schema import Schema

from .source_context import SourceContext


class TargetContext:
    relations: list[AssignNode]
    dependencies: list[DependencyNode]
    schema: Schema

    def __init__(self, relations: list[AssignNode], dependencies: list[DependencyNode]):
        self.relations = relations
        self.dependencies = dependencies
        self.schema = Schema()

        for relation in relations:
            if relation.name is None:
                raise ValueError("Relation must have a name")
            self.schema.add(relation.name, relation.attributes.names)

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

    @classmethod
    def from_syntax_tree(cls, syntax_tree: list[Node]):
        relations = []
        dependencies: list[DependencyNode] = []
        for node in syntax_tree:
            if isinstance(node, AssignNode):
                relations.append(node)
            elif isinstance(node, DependencyNode):
                dependencies.append(node)
            else:
                raise ValueError(f"Unexpected node type: {type(node)}")
        return cls(relations=relations, dependencies=dependencies)
