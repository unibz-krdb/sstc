from abc import abstractmethod
from typing import Generic, Self, TypeVar

from rapt2.treebrd.node import BinaryDependencyNode, DependencyNode, UnaryDependencyNode

from .definition import Definition

# Generic type for the node that can be either DefinitionNode or AssignNode
DefinitionType = TypeVar("DefinitionType", bound=Definition)


class Table(Generic[DefinitionType]):
    """Generic base class for source and target tables."""

    definition: DefinitionType
    dependency_nodes: list[DependencyNode]

    def __init__(self, node: DefinitionType, dependency_nodes: list[DependencyNode]):
        if node.name is None:
            raise ValueError("Node must have a name")
        self.definition = node
        self.dependency_nodes = dependency_nodes

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def attributes(self) -> list[str]:
        return self.definition.attributes

    @classmethod
    def from_relations_and_dependencies(
        cls,
        definitions: list[DefinitionType],
        dependency_nodes: list[DependencyNode],
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
            tables.append(cls(node=definition, dependency_nodes=dependencies))

        return tables

    @abstractmethod
    def create_stmt(self) -> str:
        raise NotImplementedError

    def gen_insert_table_create(self) -> str:
        return "\n".join(
            (
                f"CREATE TABLE {self.name}_INSERT AS"
                f"SELECT * FROM {self.name}"
                "WHERE 1<>1;"
            )
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
