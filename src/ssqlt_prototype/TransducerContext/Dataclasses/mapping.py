import os
from dataclasses import dataclass
from typing import Self
from jinja2 import Template


@dataclass
class Mapping:
    schema: str
    source_tables: list[str]
    target_table: str
    sql_template: Template

    @classmethod
    def from_file(cls, file_path: str) -> Self:
        filename = os.path.basename(file_path)
        tokens = filename.split(".")
        schema = tokens[0]
        target_table = tokens[-2]
        source_tables = tokens[1:-2]
        with open(file_path, "r") as f:
            sql_template = Template(f.read().strip())
        return cls(
            schema=schema,
            source_tables=source_tables,
            target_table=target_table,
            sql_template=sql_template,
        )

    def sql(self, custom_source_tables: list[str] | None = None) -> str:
        source_tables = custom_source_tables or self.source_tables
        if len(source_tables) != len(self.source_tables):
            raise ValueError(
                f"Number of source tables is insufficient: expected {len(self.source_tables)}, got {len(source_tables)}"
            )
        subsitution_mapping = {}
        for i, table in enumerate(source_tables):
            subsitution_mapping[f"S{i}"] = table
        return self.sql_template.render(**subsitution_mapping)
