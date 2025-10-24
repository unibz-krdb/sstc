import json
from typing import Self

from rapt2.rapt import Rapt
from rapt2.treebrd.node import DependencyNode

from sstc.definition import SourceDefinition

from .context import Context
from .source_table import SourceTable


class SourceContext(Context[SourceTable]):
    @classmethod
    def from_file(cls, schema_path: str, constraints_path: str) -> Self:
        schema = {}
        with open(schema_path, "r") as file:
            rich_schema = json.load(file)

        source_definitions: list[SourceDefinition] = []
        for table_schema in rich_schema["tables"]:
            table_name = table_schema["name"]
            attributes = [attr["name"] for attr in table_schema["attributes"]]
            schema[table_name] = attributes
            source_definitions.append(SourceDefinition(schema=table_schema))

        with open(constraints_path, "r") as file:
            content = file.read()

        dependencies = [
            node
            for node in Rapt(grammar="Dependency Grammar").to_syntax_tree(
                instring=content, schema=schema
            )
            if isinstance(node, DependencyNode)
        ]

        tables = SourceTable.from_relations_and_dependencies(
            definitions=source_definitions,
            dependency_nodes=dependencies,
        )

        return cls(tables=tables)
