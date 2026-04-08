"""SQL generator for the SSTC compilation pipeline.

Transforms a TransducerContext (parsed source/target relational algebra)
into executable PostgreSQL DDL: schema creation, base tables, foreign keys,
constraint enforcement (MVDs, CFDs, INCs), insert/delete tracking,
join staging, and bidirectional mapping functions with triggers.
"""

from pathlib import Path

import jinja2

from .constraints import UnsupportedError as UnsupportedError
from .constraints import constraints, foreign_keys
from .constraints import inc_sql as _inc_sql_impl
from .context import Context, Direction
from .guard import (
    GuardHierarchy,
    build_containment_pruning,
    build_guard_hierarchy,
    build_null_pattern_where,
    extract_table_guard_attrs,
)
from .table import Table
from .transducer_context import TransducerContext


SOURCE_LOOP_CHECK = -1
TARGET_LOOP_CHECK = 1
SOURCE_LOOP_VALUE = 1
TARGET_LOOP_VALUE = -1


class Generator:
    """Compiles a TransducerContext into a complete PostgreSQL SQL script.

    The compilation pipeline produces layered output: a schema preamble,
    base tables with foreign keys, constraint enforcement functions
    (MVDs, FDs/CFDs, INCs), insert/delete tracking infrastructure,
    natural-join staging, and four bidirectional mapping functions
    (source/target x insert/delete) with their triggers.
    """

    def __init__(self, ctx: TransducerContext, schema: str = "transducer"):
        self.ctx = ctx
        self.schema = schema
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
            keep_trailing_newline=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._hierarchy: GuardHierarchy | None = None

    @property
    def _universal_schema(self) -> list:
        return self.ctx.source.universal_schema

    def _universal_columns(self) -> list[dict]:
        return [
            {"name": a.name, "data_type": a.data_type} for a in self._universal_schema
        ]

    def _universal_col_names(self) -> list[str]:
        return [a.name for a in self._universal_schema]

    def compile(self) -> str:
        """Generate the full SQL script from all pipeline layers.

        Returns a single string containing schema preamble, base tables,
        foreign keys, constraints, tracking, join staging, and mapping
        sections, separated by blank lines.
        """
        if len(self.ctx.source.tables) != 1:
            raise UnsupportedError(
                f"Expected exactly 1 source table, got {len(self.ctx.source.tables)}. "
                "Multi-source-table transducers are not yet supported."
            )
        sections = [
            self._preamble(),
            self._base_tables(),
            self._foreign_keys(),
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
        schema_by_name = {a.name.lower(): a for a in self._universal_schema}
        columns = []
        for attr_name in table.attributes:
            attr = schema_by_name.get(attr_name.lower())
            if attr:
                columns.append(
                    {
                        "name": attr.name,
                        "data_type": attr.data_type,
                        "is_nullable": attr.is_nullable,
                    }
                )
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

    def _build_guard_hierarchy(self) -> GuardHierarchy:
        """Build (and cache) the specialization hierarchy from universal schema + target table guards."""
        if self._hierarchy is not None:
            return self._hierarchy
        self._hierarchy = build_guard_hierarchy(
            target_tables=self.ctx.target.tables,
            universal_schema=self._universal_schema,
            source_primary_keys=self.ctx.source.primary_keys,
        )
        return self._hierarchy

    def _extract_table_guard_attrs(self, table: Table) -> list[str]:
        """Extract guard attributes from a target table's select clause."""
        return extract_table_guard_attrs(table)

    def _inc_sql(self, context: Context) -> str:
        """Backward-compat wrapper for tests."""
        return _inc_sql_impl(context, self._render)

    def _foreign_keys(self) -> str:
        return foreign_keys(self.ctx.source, self.ctx.target, self.schema)

    def _constraints(self) -> str:
        """Generate all constraint enforcement (MVDs, FDs/CFDs, INCs) for both contexts."""
        return constraints(
            self.ctx.source,
            self.ctx.target,
            self._build_guard_hierarchy(),
            self._render,
        )

    def _tracking(self) -> str:
        """Generate insert/delete tracking infrastructure for both contexts.

        For each table in each context, produces a tracking table (shadow
        clone), a capture function (guarded by loop detection), and a
        trigger that fires AFTER INSERT or AFTER DELETE on the base table.
        """
        parts = []
        for context in [self.ctx.source, self.ctx.target]:
            direction = context.direction
            # Source checks loop_start = -1, target checks loop_start = 1
            loop_check = (
                SOURCE_LOOP_CHECK
                if direction is Direction.SOURCE
                else TARGET_LOOP_CHECK
            )
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
        """Generate join staging layer for both contexts.

        For each table, produces a JOIN staging table, a function that
        natural-joins all tracked changes into universal tuples (writing
        to the loop table for cycle detection), and a trigger that fires
        when rows land in the tracking table.
        """
        parts = []
        universal_columns = self._universal_columns()
        universal_col_names = self._universal_col_names()

        for context in [self.ctx.source, self.ctx.target]:
            direction = context.direction
            # Source inserts +1 to loop, target inserts -1
            loop_value = (
                SOURCE_LOOP_VALUE
                if direction is Direction.SOURCE
                else TARGET_LOOP_VALUE
            )

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

    @staticmethod
    def _cleanup_names(table_names: list[str], suffix: str) -> list[str]:
        """Return tracking and join staging table names to TRUNCATE after mapping."""
        result = []
        for name in table_names:
            result.extend([f"{name}_{suffix}", f"{name}_{suffix}_JOIN"])
        return result

    def _mapping_triggers(
        self, fn_name: str, table_names: list[str], suffix: str
    ) -> list[str]:
        """Render triggers that wire each table's JOIN staging table to a mapping function."""
        return [
            self._render(
                "mapping_trigger.sql.j2",
                fn_name=fn_name,
                table_name=name,
                suffix=suffix,
            )
            for name in table_names
        ]

    def _mapping(self) -> str:
        """Generate the four bidirectional mapping functions and their triggers.

        Produces SOURCE_INSERT_FN, TARGET_INSERT_FN, SOURCE_DELETE_FN,
        and TARGET_DELETE_FN. Each function reads from join staging tables,
        applies the appropriate mapping (project universal tuples into the
        opposite context's tables), and cleans up tracking state. Insert
        mappings use containment pruning and null-pattern filtering;
        delete mappings use MVD independence checks.
        """
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
                "guard_check": " AND ".join(
                    f"{a} IS NOT NULL" for a in extract_table_guard_attrs(t)
                ),
            }
            for t in target.tables
        ]

        hierarchy = self._build_guard_hierarchy()

        # Cleanup table names for each direction + suffix
        src_cleanup = self._cleanup_names(src_table_names, "INSERT")
        tgt_cleanup = self._cleanup_names(tgt_table_names, "INSERT")
        src_del_cleanup = self._cleanup_names(src_table_names, "DELETE")
        tgt_del_cleanup = self._cleanup_names(tgt_table_names, "DELETE")

        # Per-table WHERE: source PK columns + each target table's PK columns
        src_pk_cols = hierarchy.source_pk
        for info in tgt_tables_info:
            where_cols = list(src_pk_cols)
            for pk_col in info["pk"]:
                if pk_col not in where_cols:
                    where_cols.append(pk_col)
            info["where_not_null"] = " AND ".join(
                f"{a} IS NOT NULL" for a in where_cols
            )
        tgt_insert_where = build_null_pattern_where(hierarchy)

        # --- SOURCE_INSERT_FN ---
        parts.append(
            self._render(
                "insert_mapping.sql.j2",
                fn_name="SOURCE_INSERT_FN",
                suffix="INSERT",
                source_tables=src_table_names,
                target_tables=tgt_tables_info,
                where_not_null="",
                cleanup_tables=src_cleanup,
                use_temp_join=False,
                universal_columns=universal_columns,
                universal_col_names=universal_col_names,
                loop_value=None,
            )
        )
        parts.extend(
            self._mapping_triggers("SOURCE_INSERT_FN", src_table_names, "INSERT")
        )

        # --- TARGET_INSERT_FN ---
        prune_rules = build_containment_pruning(hierarchy)
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
                prune_rules=prune_rules,
            )
        )
        parts.extend(
            self._mapping_triggers("TARGET_INSERT_FN", tgt_table_names, "INSERT")
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
                use_abs=False,
            )
        )
        parts.extend(
            self._mapping_triggers("SOURCE_DELETE_FN", src_table_names, "DELETE")
        )

        # --- TARGET_DELETE_FN ---
        tgt_delete_checks = self._build_target_delete_checks(source)
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
                use_abs=True,
            )
        )
        parts.extend(
            self._mapping_triggers("TARGET_DELETE_FN", tgt_table_names, "DELETE")
        )

        return "\n\n".join(parts)

    def _build_source_delete_checks(self, source: Context, target: Context) -> dict:
        """Build independence checks for source->target DELETE mapping."""
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

    def _build_target_delete_checks(self, source: Context) -> list[dict]:
        """Build independence checks for target->source DELETE mapping.

        Joins SOURCE base tables (not target) to check whether removing
        a universal tuple leaves other source tuples that still require
        the same data.
        """
        src_names = [t.name for t in source.tables]
        join_source = "SELECT * FROM " + " NATURAL LEFT OUTER JOIN ".join(
            f"{self.schema}._{n}" for n in src_names
        )
        join_cols = ", ".join(f"r1.{a}" for a in self._universal_col_names())

        nullable_set = set(self._build_guard_hierarchy().nullable_cols)

        def _eq(a: str) -> str:
            if a in nullable_set:
                return f"r1.{a} IS NOT DISTINCT FROM temp_table_join.{a}"
            return f"r1.{a} = temp_table_join.{a}"

        if len(source.tables) == 1:
            src = source.tables[0]
            src_pk = source.primary_keys.get(src.name, [])
            join_cond = " AND ".join(_eq(a) for a in src.attributes)
            return [
                {
                    "main_table": src.name,
                    "pk": src_pk,
                    "join_source": join_source,
                    "join_cols": join_cols,
                    "join_condition": join_cond,
                    "dependent_deletes": [],
                }
            ]

        # Multi-source: main table always deleted, dependent tables conditionally
        main = source.tables[0]
        main_pk = source.primary_keys.get(main.name, [])
        checks = []
        for dep in source.tables[1:]:
            dep_pk = source.primary_keys.get(dep.name, [])
            join_cond = " AND ".join(_eq(a) for a in dep.attributes)
            checks.append(
                {
                    "main_table": main.name,
                    "pk": main_pk,
                    "join_source": join_source,
                    "join_cols": join_cols,
                    "join_condition": join_cond,
                    "dependent_deletes": [{"name": dep.name, "pk": dep_pk}],
                }
            )
        return checks
