from pathlib import Path

import jinja2

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
        ]
        return "\n\n".join(s for s in sections if s)

    def _render(self, template_name: str, **kwargs) -> str:
        template = self.env.get_template(template_name)
        return template.render(schema=self.schema, **kwargs)

    def _preamble(self) -> str:
        return self._render("preamble.sql.j2")
