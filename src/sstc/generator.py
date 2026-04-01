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

    def _universal_columns(self) -> list[dict]:
        schema = self.ctx.source.tables[0].universal_schema
        return [{"name": a.name, "data_type": a.data_type} for a in schema]

    def _universal_col_names(self) -> list[str]:
        return [c["name"] for c in self._universal_columns()]

    def compile(self) -> str:
        sections = [
            self._preamble(),
            self._base_tables(),
            self._constraints(),
            self._tracking(),
            self._join(),
            self._mapping(),
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

    def _join(self) -> str:
        parts = []
        universal_columns = self._universal_columns()
        universal_col_names = self._universal_col_names()

        for context in [self.ctx.source, self.ctx.target]:
            direction = context.direction
            # Source inserts +1 to loop, target inserts -1
            loop_value = 1 if direction == "source" else -1

            all_tables_info = [
                {"name": t.name, "attrs": t.attributes} for t in context.tables
            ]

            for table in context.tables:
                other_tables = [t.name for t in context.tables if t.name != table.name]

                for suffix in ["INSERT", "DELETE"]:
                    # JOIN staging table (empty clone of base table)
                    parts.append(
                        self._render(
                            "tracking_table.sql.j2",
                            table_name=table.name,
                            suffix=f"{suffix}_JOIN",
                        )
                    )
                    # JOIN function
                    parts.append(
                        self._render(
                            "join_function.sql.j2",
                            direction=direction,
                            table_name=table.name,
                            suffix=suffix,
                            universal_columns=universal_columns,
                            universal_col_names=universal_col_names,
                            other_tables=other_tables,
                            all_tables=all_tables_info,
                            loop_value=loop_value,
                        )
                    )
                    # JOIN trigger (fires on INSERT into _TABLE_INSERT/DELETE)
                    parts.append(
                        self._render(
                            "join_trigger.sql.j2",
                            direction=direction,
                            table_name=table.name,
                            suffix=suffix,
                        )
                    )
        return "\n\n".join(parts)

    def _mapping(self) -> str:
        parts = []
        source = self.ctx.source
        target = self.ctx.target
        universal_columns = self._universal_columns()
        universal_col_names = self._universal_col_names()

        src_table_names = [t.name for t in source.tables]
        tgt_table_names = [t.name for t in target.tables]

        src_tables_info = [
            {
                "name": t.name,
                "attrs": t.attributes,
                "pk": source.primary_keys.get(t.name, []),
            }
            for t in source.tables
        ]
        tgt_tables_info = [
            {
                "name": t.name,
                "attrs": t.attributes,
                "pk": target.primary_keys.get(t.name, []),
            }
            for t in target.tables
        ]

        # Cleanup table names for each direction + suffix
        src_cleanup = []
        for name in src_table_names:
            src_cleanup.extend([f"{name}_INSERT", f"{name}_INSERT_JOIN"])
        tgt_cleanup = []
        for name in tgt_table_names:
            tgt_cleanup.extend([f"{name}_INSERT", f"{name}_INSERT_JOIN"])
        src_del_cleanup = []
        for name in src_table_names:
            src_del_cleanup.extend([f"{name}_DELETE", f"{name}_DELETE_JOIN"])
        tgt_del_cleanup = []
        for name in tgt_table_names:
            tgt_del_cleanup.extend([f"{name}_DELETE", f"{name}_DELETE_JOIN"])

        # WHERE condition: all columns from source tables must be NOT NULL
        src_where_cols = set()
        for t in source.tables:
            src_where_cols.update(t.attributes)
        src_insert_where = " AND ".join(f"{a} IS NOT NULL" for a in src_where_cols)
        tgt_insert_where = " AND ".join(f"{a} IS NOT NULL" for a in universal_col_names)

        # --- SOURCE_INSERT_FN ---
        parts.append(
            self._render(
                "insert_mapping.sql.j2",
                fn_name="SOURCE_INSERT_FN",
                suffix="INSERT",
                source_tables=src_table_names,
                target_tables=tgt_tables_info,
                where_not_null=src_insert_where,
                cleanup_tables=src_cleanup,
                use_temp_join=False,
                universal_columns=universal_columns,
                universal_col_names=universal_col_names,
                loop_value=None,
            )
        )
        for name in src_table_names:
            parts.append(
                self._render(
                    "mapping_trigger.sql.j2",
                    fn_name="SOURCE_INSERT_FN",
                    table_name=name,
                    suffix="INSERT",
                )
            )

        # --- TARGET_INSERT_FN ---
        parts.append(
            self._render(
                "insert_mapping.sql.j2",
                fn_name="TARGET_INSERT_FN",
                suffix="INSERT",
                source_tables=tgt_table_names,
                target_tables=src_tables_info,
                where_not_null=tgt_insert_where,
                cleanup_tables=tgt_cleanup,
                use_temp_join=True,
                universal_columns=universal_columns,
                universal_col_names=universal_col_names,
                loop_value=-1,
            )
        )
        for name in tgt_table_names:
            parts.append(
                self._render(
                    "mapping_trigger.sql.j2",
                    fn_name="TARGET_INSERT_FN",
                    table_name=name,
                    suffix="INSERT",
                )
            )

        # --- SOURCE_DELETE_FN ---
        mvd_checks = self._build_source_delete_checks(source, target)
        parts.append(
            self._render(
                "delete_mapping.sql.j2",
                fn_name="SOURCE_DELETE_FN",
                source_tables=src_table_names,
                independence_checks=mvd_checks.get("mvd_checks", []),
                full_independence_check=mvd_checks.get("full_independence_check"),
                cleanup_tables=src_del_cleanup,
                use_temp_join=False,
                universal_columns=universal_columns,
                universal_col_names=universal_col_names,
                where_not_null="",
            )
        )
        for name in src_table_names:
            parts.append(
                self._render(
                    "mapping_trigger.sql.j2",
                    fn_name="SOURCE_DELETE_FN",
                    table_name=name,
                    suffix="DELETE",
                )
            )

        # --- TARGET_DELETE_FN ---
        tgt_delete_checks = self._build_target_delete_checks(source, target)
        parts.append(
            self._render(
                "delete_mapping.sql.j2",
                fn_name="TARGET_DELETE_FN",
                source_tables=tgt_table_names,
                independence_checks=tgt_delete_checks,
                full_independence_check=None,
                cleanup_tables=tgt_del_cleanup,
                use_temp_join=True,
                universal_columns=universal_columns,
                universal_col_names=universal_col_names,
                where_not_null=tgt_insert_where,
            )
        )
        for name in tgt_table_names:
            parts.append(
                self._render(
                    "mapping_trigger.sql.j2",
                    fn_name="TARGET_DELETE_FN",
                    table_name=name,
                    suffix="DELETE",
                )
            )

        return "\n\n".join(parts)

    def _build_source_delete_checks(self, source: Context, target: Context) -> dict:
        """Build independence checks for source→target DELETE mapping."""
        mvds = source.multivalued_dependencies
        src_table = source.tables[0]
        src_table_names = [t.name for t in source.tables]
        src_name = src_table.name
        all_attrs = src_table.attributes
        pk = source.primary_keys.get(src_name, [])

        mvd_checks = []
        for mvd in mvds:
            lhs_attrs = list(mvd.attributes)[:-1]
            det_attr = list(mvd.attributes)[-1]

            # Find target table whose attrs contain the MVD determined attr + LHS
            target_table = None
            for t in target.tables:
                if det_attr in t.attributes and all(
                    a in t.attributes for a in lhs_attrs
                ):
                    target_table = t
                    break

            if target_table is None:
                continue

            target_pk = target.primary_keys.get(target_table.name, [])
            lhs_match = " AND ".join(f"{a} = NEW.{a}" for a in lhs_attrs)
            det_match = f"{det_attr} = NEW.{det_attr}"

            mvd_checks.append(
                {
                    "source_table": src_name,
                    "target_table": target_table.name,
                    "target_pk": target_pk,
                    "lhs_match": lhs_match,
                    "det_match": det_match,
                    "join_expr": " NATURAL LEFT OUTER JOIN ".join(
                        f"{self.schema}._{n}_DELETE_JOIN" for n in src_table_names
                    ),
                }
            )

        # Full independence check: if no tuples remain with same PK, delete all
        non_pk_attrs = [a for a in all_attrs if a not in pk]
        lhs_match = " AND ".join(f"{a} = NEW.{a}" for a in pk)
        all_match = (
            lhs_match + " AND " + " AND ".join(f"{a} = NEW.{a}" for a in non_pk_attrs)
        )

        full_deletes = []
        for t in target.tables:
            t_pk = target.primary_keys.get(t.name, [])
            cond = " AND ".join(
                f"{a} = NEW.{a}" for a in (t_pk if t_pk else t.attributes)
            )
            full_deletes.append({"table": t.name, "condition": cond})

        return {
            "mvd_checks": mvd_checks,
            "full_independence_check": {
                "source_table": src_name,
                "lhs_match": lhs_match,
                "all_match": all_match,
                "deletes": full_deletes,
            },
        }

    def _build_target_delete_checks(
        self, source: Context, target: Context
    ) -> list[dict]:
        """Build independence checks for target→source DELETE mapping."""
        checks = []
        for src_table in source.tables:
            src_pk = source.primary_keys.get(src_table.name, [])
            tgt_names = [t.name for t in target.tables]
            join_source = " NATURAL LEFT OUTER JOIN ".join(
                f"{self.schema}._{n}" for n in tgt_names
            )
            join_cols = ", ".join(f"r1.{a}" for a in self._universal_col_names())
            join_cond = " AND ".join(f"r1.{a} = temp_table_join.{a}" for a in src_pk)
            checks.append(
                {
                    "main_table": src_table.name,
                    "pk": src_pk,
                    "join_source": join_source,
                    "join_cols": join_cols,
                    "join_condition": join_cond,
                    "dependent_deletes": [],
                }
            )
        return checks
