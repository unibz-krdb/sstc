import json
from typing import Self

from rapt2.rapt import Rapt
from rapt2.treebrd.node import AssignNode, DependencyNode
from rapt2.treebrd.schema import Schema

from sstc.definition import AttributeSchema

from .table import Table


class Context:
    """Generic base class for source and target contexts."""

    tables: list[Table]
    schema: Schema

    def __init__(self, tables: list[Table]):
        self.tables = tables
        self.schema = Schema()
        for table in tables:
            self.schema.add(table.name, table.attributes)

    @classmethod
    def from_file(cls, universal_path: str, context_path: str) -> Self:

        universal_attributes = []
        schema = {"Universal": []}
        with open(universal_path, "r") as file:
            universal_schema = json.load(file)
            for json_attr in universal_schema:
                universal_attributes.append(AttributeSchema.from_dict(json_attr))
                schema["Universal"].append(json_attr["name"])

        with open(context_path, "r") as file:
            content = file.read()

        relations: list[AssignNode] = []
        dependencies: list[DependencyNode] = []
        universal_mapping: None | AssignNode = None

        for node in Rapt(grammar="Dependency Grammar").to_syntax_tree(
            instring=content,
            schema=schema,
        ):
            if isinstance(node, AssignNode):
                if node.name.lower() == "UniversalMapping".lower():
                    universal_mapping = node
                else:
                    relations.append(node)
            elif isinstance(node, DependencyNode):
                dependencies.append(node)
            else:
                raise ValueError(f"Unexpected node type: {type(node)}")

        if universal_mapping is None:
            raise Exception("UniversalMapping is not defined.")

        tables = Table.from_relations_and_dependencies(
            definitions=relations,
            dependency_nodes=dependencies,
            universal_schema=universal_attributes,
            universal_mapping=universal_mapping,
        )

        return cls(tables=tables)
