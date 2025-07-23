from dataclasses import dataclass

from .constraint import Constraint
from .table import Table, Attr

class Graph:
    def __init__(self):
        self.nodes: dict[str, set] = {}

    def add_node(self, name: str):
        if name not in self.nodes:
            self.nodes[name] = set()

    def add_edge(self, from_name: str, to_name: str):

        self.add_node(from_name)
        self.add_node(to_name)
        self.nodes[from_name].add(to_name)

def topological_sort(graph: Graph) -> list[str]:
    visited = set()
    temp = set()
    result = []

    def visit(node: str):
        if node in temp:
            raise ValueError(f"Circular dependency detected at {node}")
        if node not in visited:
            temp.add(node)
            for neighbor in graph.nodes.get(node, []):
                visit(neighbor)
            temp.remove(node)
            visited.add(node)
            result.append(node)

    for node in graph.nodes:
        visit(node)

    return result[::-1]

@dataclass
class DbContext:
    schema: str
    tables: dict[str, Table]
    constraints: dict[str, list[Constraint]]
    ordering: list[str]

    @classmethod
    def from_files(
        cls,
        create_paths: list[str],
        constraint_paths: list[str],
        mapping_paths: list[str],
    ) -> "DbContext":

        tables: dict[str, Table] = {}
        for file_path in create_paths:
            found = False
            filename = file_path.split("/")[-1].split(".")[0]
            for mapping_path in mapping_paths:
                mapping_filename = mapping_path.split("/")[-1].split(".")[0]
                if filename == mapping_filename:
                    create_table = Table.from_create_path(file_path, mapping_path=mapping_path)
                    tables[create_table.name] = create_table
                    found = True
                    break
            if not found:
                raise ValueError(
                    f"Mapping file not found for table: {filename}. "
                    "Ensure that the mapping file has the same name as the create file."
                )

        schema = ""
        for table in tables.values():
            if schema == "":
                schema = table.schema
            elif schema != table.schema:
                raise ValueError(
                    "All tables must have the same schema. "
                    f"Found different schemas: {schema} and {table.schema}."
                )

        constraints = {}
        for file_path in constraint_paths:
            constraint = Constraint.from_file(file_path)
            if constraint.table not in constraints:
                constraints[constraint.table] = []
            constraints[constraint.table].append(constraint)

        depGraph = Graph()
        for table in tables.values():
            depGraph.add_node(table.name)
            for fkey in table.fkey:
                depGraph.add_edge(fkey.ref_tablename, table.name)

        path = topological_sort(depGraph)

        return cls(
            tables=tables,
            constraints=constraints,
            ordering=path,
            schema=schema
        )

    def all_attributes(self) -> list[Attr]:
        attributes = []
        for tablename in reversed(self.ordering):
            table = self.tables[tablename]
            for attr in table.attributes:
                if attr not in attributes:
                    attributes.append(attr)
        return attributes

    def create_temp_table(self, name: str) -> str:
        result = ""
        all_attributes = self.all_attributes()
        result += "\n" + f"create temporary table {name} (\n"
        result += ",\n".join(f"\t{attr.name} {attr._type}" for attr in all_attributes)
        result += "\n);"
        return result
