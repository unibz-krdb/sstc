from pathlib import Path

import jinja2

from .context import Context
from .table import Table
from .transducer_context import TransducerContext


class Generator:
    def __init__(self, ctx: TransducerContext, schema: str = "transducer"):
        self.ctx = ctx
        self.schema = schema
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
            keep_trailing_newline=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def compile(self) -> str:
        sections = [
            self._preamble(),
            self._base_tables(),
        ]
        return "\n\n".join(s for s in sections if s)

    def _render(self, template_name: str, **kwargs) -> str:
        template = self.env.get_template(template_name)
        return template.render(schema=self.schema, **kwargs)

    def _preamble(self) -> str:
        return self._render("preamble.sql.j2")

    def _table_columns(self, table: Table) -> list[dict]:
        columns = []
        for attr_name in table.attributes:
            for attr in table.universal_schema:
                if attr.name.lower() == attr_name.lower():
                    columns.append(
                        {
                            "name": attr.name,
                            "data_type": attr.data_type,
                            "is_nullable": attr.is_nullable,
                        }
                    )
                    break
        return columns

    def _create_table(self, table: Table, context: Context) -> str:
        pk_columns = context.primary_keys.get(table.name, [])
        return self._render(
            "create_table.sql.j2",
            table_name=table.name,
            columns=self._table_columns(table),
            pk_columns=pk_columns,
        )

    def _base_tables(self) -> str:
        parts = []
        for context in [self.ctx.source, self.ctx.target]:
            for table in context.tables:
                parts.append(self._create_table(table, context))
        return "\n".join(parts)
