from typing import Self

from rapt2.rapt import sql_translator
from rapt2.treebrd.node import (
    AssignNode,
    BinaryDependencyNode,
    DependencyNode,
    UnaryDependencyNode,
)

from .definition import AttributeSchema


class Table:
    """Generic base class for source and target tables."""

    definition: AssignNode
    dependency_nodes: list[DependencyNode]
    universal_schema: list[AttributeSchema]
    universal_mapping: AssignNode

    def __init__(
        self,
        node: AssignNode,
        dependency_nodes: list[DependencyNode],
        universal_schema: list[AttributeSchema],
        universal_mapping: AssignNode,
    ):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.definition = node
        self.dependency_nodes = dependency_nodes
        self.universal_schema = universal_schema
        self.universal_mapping = universal_mapping

    @property
    def name(self) -> str:
        if self.definition.name is None:
            raise ValueError("Node must have a name")
        return self.definition.name

    @property
    def attributes(self) -> list[str]:
        return self.definition.attributes.names

    @classmethod
    def from_relations_and_dependencies(
        cls,
        definitions: list[AssignNode],
        dependency_nodes: list[DependencyNode],
        universal_schema: list[AttributeSchema],
        universal_mapping: AssignNode,
    ) -> list[Self]:
        """Create tables from a list of relation nodes and dependency nodes."""
        tables: list[Self] = []
        for definition in definitions:
            dependencies: list[DependencyNode] = []
            for dependency_node in dependency_nodes:
                if isinstance(dependency_node, UnaryDependencyNode):
                    if dependency_node.relation_name == definition.name:
                        dependencies.append(dependency_node)
                elif isinstance(dependency_node, BinaryDependencyNode):
                    if (
                        dependency_node.left_child.name == definition.name
                        or dependency_node.right_child.name == definition.name
                    ):
                        dependencies.append(dependency_node)
            tables.append(
                cls(
                    node=definition,
                    dependency_nodes=dependencies,
                    universal_schema=universal_schema,
                    universal_mapping=universal_mapping,
                )
            )

        return tables

    def gen_concrete_create_stmt(self) -> str:
        table_name = self.name
        columns = []

        for my_attr in self.attributes:
            for attr in self.universal_schema:
                if attr.name.lower() != my_attr.lower():
                    continue
                column_def = f"{attr.name} {attr.data_type}"
                if not attr.is_nullable:
                    column_def += " NOT NULL"
                columns.append(column_def)

        columns_str = ",\n    ".join(columns)
        return f"CREATE TABLE {table_name} (\n    {columns_str}\n)"

    def gen_universal_create_stmt(self) -> str:
        return sql_translator.translate(
            root_list=[self.definition], use_bag_semantics=True
        )[0].replace("TEMPORARY TABLE", "TABLE")

    def gen_insert_table_create(self) -> str:
        return (
            f"CREATE TABLE {self.name}_INSERT AS SELECT * FROM {self.name} WHERE 1<>1;"
        )

    def gen_insert_join_table_create(self) -> str:
        return self.gen_insert_table_create().replace("INSERT", "INSERT_JOIN")

    def gen_insert_function(self) -> str:
        return "\n".join(
            (
                f"CREATE OR REPLACE FUNCTION {self.name}_INSERT_fn()",
                "   RETURNS TRIGGER LANGUAGE PLPGSQL AS $$",
                "   BEGIN",
                f"   RAISE NOTICE 'Function {self.name}_INSERT_fn called';",
                "   IF EXISTS (SELECT * FROM _loop where loop_start = -1) THEN",
                "      RETURN NULL;",
                "   ELSE",
                f"      INSERT INTO {self.name}_INSERT VALUES({', '.join(f'new.{attr}' for attr in self.attributes)});",
                "      RETURN NEW;",
                "   END IF;",
                "END;  $$",
            )
        )

    def gen_insert_trigger(self) -> str:
        return "\n".join(
            (
                f"CREATE TRIGGER {self.name}_INSERT_trigger",
                f"AFTER INSERT ON {self.name}",
                "FOR EACH ROW",
                f"EXECUTE FUNCTION {self.name}_INSERT_fn();",
            )
        )
