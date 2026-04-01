from pathlib import Path

import jinja2

from rapt2.treebrd.condition_node import (
    BinaryConditionNode,
    UnaryConditionNode,
    UnaryConditionalOperator,
)
from rapt2.treebrd.node import SelectNode

from .context import Context
from .table import Table
from .transducer_context import TransducerContext


class UnsupportedError(Exception):
    pass


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
            self._constraints(),
            self._tracking(),
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

    def _mvd_sql(self, context: Context) -> str:
        mvds = context.multivalued_dependencies
        if not mvds:
            return ""

        # Group MVDs by table name
        mvds_by_table: dict[str, list] = {}
        for mvd in mvds:
            mvds_by_table.setdefault(mvd.relation_name, []).append(mvd)

        parts = []
        for table_name, table_mvds in mvds_by_table.items():
            # RAPT2 convention: mvd_{a, b} stores attributes as [a, b]
            # where attrs[:-1] = LHS determinant, attrs[-1:] = determined attribute
            lhs_set = {tuple(list(m.attributes)[:-1]) for m in table_mvds}
            if len(lhs_set) > 1:
                raise UnsupportedError(
                    f"Non-shared-LHS MVDs on {table_name}: {lhs_set}"
                )
            lhs_attrs = list(lhs_set.pop())
            determined_attrs = [list(m.attributes)[-1] for m in table_mvds]

            # Find table object for attribute list
            table = self._find_table(table_name, context)
            all_attrs = table.attributes

            # MVD check: r1 for LHS+determined, r2 for rest
            lhs_and_determined = set(lhs_attrs) | set(determined_attrs)
            select_cols = ", ".join(
                f"r1.{a}" if a in lhs_and_determined else f"r2.{a}" for a in all_attrs
            )
            new_cols = ", ".join(f"NEW.{a}" for a in all_attrs)
            join_condition = " AND ".join(f"r1.{a} = r2.{a}" for a in lhs_attrs)

            parts.append(
                self._render(
                    "mvd_check.sql.j2",
                    table_name=table_name,
                    select_cols=select_cols,
                    new_cols=new_cols,
                    join_condition=join_condition,
                )
            )

            # MVD grounding: one UNION SELECT per determined attr
            # Each SELECT swaps that determined attr with NEW, keeps rest from r1
            union_selects = []
            for det_attr in determined_attrs:
                cols = ", ".join(
                    f"NEW.{a}" if a == det_attr else f"r1.{a}" for a in all_attrs
                )
                union_selects.append({"cols": cols})

            grounding_join = " AND ".join(f"r1.{a} = NEW.{a}" for a in lhs_attrs)

            parts.append(
                self._render(
                    "mvd_grounding.sql.j2",
                    table_name=table_name,
                    union_selects=union_selects,
                    join_condition=grounding_join,
                )
            )

        return "\n\n".join(parts)

    def _find_table(self, name: str, context: Context) -> Table:
        for table in context.tables:
            if table.name == name:
                return table
        raise ValueError(f"Table {name} not found in context")

    @staticmethod
    def _extract_defined_attrs(cond) -> list[str]:
        if isinstance(cond, UnaryConditionNode):
            if cond.op == UnaryConditionalOperator.DEFINED:
                return cond.child.attribute_references()
            return []
        if isinstance(cond, BinaryConditionNode):
            return Generator._extract_defined_attrs(
                cond.left
            ) + Generator._extract_defined_attrs(cond.right)
        return []

    def _fd_sql(self, context: Context) -> str:
        fds = context.functional_dependencies
        if not fds:
            return ""

        parts = []
        for i, fd in enumerate(fds, 1):
            # RAPT2 convention: fd_{a, b} stores [a, b]
            # attrs[:-1] = LHS, attrs[-1:] = RHS
            lhs_attrs = list(fd.attributes)[:-1]
            rhs_attrs = list(fd.attributes)[-1:]

            table = self._find_table(fd.relation_name, context)
            all_attrs = table.attributes
            new_cols = ", ".join(f"NEW.{a}" for a in all_attrs)

            lhs_condition = " AND ".join(f"r1.{a} = r2.{a}" for a in lhs_attrs)
            rhs_condition = " AND ".join(f"r1.{a} <> r2.{a}" for a in rhs_attrs)

            # Extract guard attributes if FD is guarded (child is SelectNode)
            guard_attrs = []
            if isinstance(fd.child, SelectNode):
                guard_attrs = self._extract_defined_attrs(fd.child.conditions)

            parts.append(
                self._render(
                    "fd_check.sql.j2",
                    table_name=fd.relation_name,
                    fd_index=i,
                    new_cols=new_cols,
                    lhs_attrs=lhs_attrs,
                    rhs_attrs=rhs_attrs,
                    lhs_condition=lhs_condition,
                    rhs_condition=rhs_condition,
                    guard_attrs=guard_attrs,
                )
            )

        return "\n\n".join(parts)

    def _constraints(self) -> str:
        parts = []
        for context in [self.ctx.source, self.ctx.target]:
            mvd = self._mvd_sql(context)
            if mvd:
                parts.append(mvd)
            fd = self._fd_sql(context)
            if fd:
                parts.append(fd)
        return "\n\n".join(parts) if parts else ""

    def _tracking(self) -> str:
        parts = []
        for context in [self.ctx.source, self.ctx.target]:
            direction = context.direction
            # Source checks loop_start = -1, target checks loop_start = 1
            loop_check = -1 if direction == "source" else 1
            for table in context.tables:
                for suffix, event, row_prefix, return_val in [
                    ("INSERT", "AFTER INSERT", "NEW", "NEW"),
                    ("DELETE", "AFTER DELETE", "OLD", "OLD"),
                ]:
                    row_values = ", ".join(
                        f"{row_prefix}.{a}" for a in table.attributes
                    )
                    parts.append(
                        self._render(
                            "tracking_table.sql.j2",
                            table_name=table.name,
                            suffix=suffix,
                        )
                    )
                    parts.append(
                        self._render(
                            "capture_function.sql.j2",
                            direction=direction,
                            table_name=table.name,
                            suffix=suffix,
                            loop_check=loop_check,
                            row_values=row_values,
                            return_value=return_val,
                        )
                    )
                    parts.append(
                        self._render(
                            "capture_trigger.sql.j2",
                            direction=direction,
                            table_name=table.name,
                            suffix=suffix,
                            event=event,
                        )
                    )
        return "\n\n".join(parts)
