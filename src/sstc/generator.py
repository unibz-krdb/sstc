from dataclasses import dataclass, field
from pathlib import Path

import jinja2

from rapt2.treebrd.condition_node import (
    BinaryConditionNode,
    UnaryConditionNode,
    UnaryConditionalOperator,
)
from rapt2.treebrd.node import SelectNode, UnaryNode

from .context import Context
from .table import Table
from .transducer_context import TransducerContext


class UnsupportedError(Exception):
    pass


@dataclass
class GuardLevel:
    guard_attrs: set[str]
    tables: list[str] = field(default_factory=list)
    not_null_cols: list[str] = field(default_factory=list)
    null_cols: list[str] = field(default_factory=list)


@dataclass
class GuardHierarchy:
    mandatory_cols: list[str]
    nullable_cols: list[str]
    levels: list[GuardLevel]
    source_pk: list[str]


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
        self._hierarchy: GuardHierarchy | None = None

    @property
    def _universal_schema(self) -> list:
        return self.ctx.source.tables[0].universal_schema

    def _universal_columns(self) -> list[dict]:
        return [{"name": a.name, "data_type": a.data_type} for a in self._universal_schema]

    def _universal_col_names(self) -> list[str]:
        return [a.name for a in self._universal_schema]

    def compile(self) -> str:
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

    def _foreign_keys(self) -> str:
        parts: list[str] = []
        for context in [self.ctx.source, self.ctx.target]:
            pks = context.primary_keys

            # inc=_{a, b} (T1, T2): T2[b] references T1[a]
            for inc in context.inclusion_equivalences:
                names = list(inc.relation_names)
                attrs = list(inc.attributes)
                mid = len(attrs) // 2
                referenced_table, referencing_table = names[0], names[1]
                referenced_cols, referencing_cols = attrs[:mid], attrs[mid:]
                ref_pk = pks.get(referenced_table, [])
                if sorted(referenced_cols) == sorted(ref_pk):
                    parts.append(
                        f"ALTER TABLE {self.schema}._{referencing_table} "
                        f"ADD FOREIGN KEY ({', '.join(referencing_cols)}) "
                        f"REFERENCES {self.schema}._{referenced_table}"
                        f" ({', '.join(referenced_cols)});"
                    )

            # inc⊆_{a, b} (T1, T2): T1[a] references T2[b]
            for inc in context.inclusion_subsumptions:
                names = list(inc.relation_names)
                attrs = list(inc.attributes)
                mid = len(attrs) // 2
                referencing_table, referenced_table = names[0], names[1]
                referencing_cols, referenced_cols = attrs[:mid], attrs[mid:]
                ref_pk = pks.get(referenced_table, [])
                if sorted(referenced_cols) == sorted(ref_pk):
                    parts.append(
                        f"ALTER TABLE {self.schema}._{referencing_table} "
                        f"ADD FOREIGN KEY ({', '.join(referencing_cols)}) "
                        f"REFERENCES {self.schema}._{referenced_table}"
                        f" ({', '.join(referenced_cols)});"
                    )

        return "\n".join(parts) if parts else ""

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

    def _extract_table_guard_attrs(self, table: Table) -> list[str]:
        """Extract guard attributes from a target table's select clause."""
        node = table.definition.child  # Skip AssignNode → get ProjectNode
        while node is not None:
            if isinstance(node, SelectNode):
                return self._extract_defined_attrs(node.conditions)
            if isinstance(node, UnaryNode):
                node = node.child
            else:
                return []
        return []

    def _build_guard_hierarchy(self) -> GuardHierarchy:
        """Build (and cache) the specialization hierarchy from universal schema + target table guards."""
        if self._hierarchy is not None:
            return self._hierarchy

        schema = self._universal_schema
        mandatory_cols = [a.name for a in schema if not a.is_nullable]
        nullable_cols = [a.name for a in schema if a.is_nullable]

        # Extract distinct guard sets from target tables
        guard_sets: dict[frozenset[str], list[str]] = {}
        for table in self.ctx.target.tables:
            guard = frozenset(self._extract_table_guard_attrs(table))
            guard_sets.setdefault(guard, []).append(table.name)

        # Always include empty guard (Level 0)
        if frozenset() not in guard_sets:
            guard_sets[frozenset()] = []

        # Sort by cardinality ascending
        sorted_guards = sorted(guard_sets.items(), key=lambda x: len(x[0]))

        # Build levels with cumulative not_null / null columns
        levels = []
        for guard_frozen, tables in sorted_guards:
            cumulative = set()
            for g, _ in sorted_guards:
                if g <= guard_frozen:
                    cumulative |= set(g)

            not_null = [c for c in nullable_cols if c in cumulative]
            null = [c for c in nullable_cols if c not in cumulative]

            levels.append(
                GuardLevel(
                    guard_attrs=set(guard_frozen),
                    tables=tables,
                    not_null_cols=not_null,
                    null_cols=null,
                )
            )

        src_pk: list[str] = []
        for t in self.ctx.source.tables:
            for col in self.ctx.source.primary_keys.get(t.name, []):
                if col not in src_pk:
                    src_pk.append(col)

        self._hierarchy = GuardHierarchy(
            mandatory_cols=mandatory_cols,
            nullable_cols=nullable_cols,
            levels=levels,
            source_pk=src_pk,
        )
        return self._hierarchy

    @staticmethod
    def _build_cfd_where_branches(
        lhs_attrs: list[str],
        rhs_attrs: list[str],
        guard_attrs: list[str],
        hierarchy: GuardHierarchy,
    ) -> list[str]:
        """Build exhaustive WHERE OR-branches for a CFD check.

        Uses the guard hierarchy to determine which null-patterns are valid
        at each specialization level, generating branches only for states
        that genuinely violate the hierarchy.
        """
        branches: list[str] = []

        # Branch 1: Main FD violation — all guards non-NULL, LHS match, RHS differ
        guard_not_null = " AND ".join(f"R2.{a} IS NOT NULL" for a in guard_attrs)
        lhs_match = " AND ".join(f"R1.{a} = R2.{a}" for a in lhs_attrs)
        rhs_differ = " AND ".join(f"R1.{a} <> R2.{a}" for a in rhs_attrs)
        branches.append(f"({guard_not_null} AND {lhs_match} AND {rhs_differ})")

        # Find level-groups: new attrs added at each hierarchy level
        level_groups: list[list[str]] = []
        for i, level in enumerate(hierarchy.levels):
            prev = set(hierarchy.levels[i - 1].not_null_cols) if i > 0 else set()
            new = [c for c in level.not_null_cols if c not in prev]
            if new:
                level_groups.append(new)

        def find_group(attr: str) -> list[str]:
            for group in level_groups:
                if attr in group:
                    return group
            return [attr]

        def find_group_index(attr: str) -> int:
            for i, group in enumerate(level_groups):
                if attr in group:
                    return i
            return -1

        rhs_group = find_group(rhs_attrs[0])
        lhs_idx = find_group_index(lhs_attrs[0])
        rhs_idx = find_group_index(rhs_attrs[0])
        cross_level = lhs_idx != rhs_idx

        # Cross-level: LHS NULL → no RHS-group attr can be NOT NULL
        if cross_level:
            for x in lhs_attrs:
                for r in rhs_group:
                    b = f"(R2.{x} IS NULL AND R2.{r} IS NOT NULL)"
                    if b not in branches:
                        branches.append(b)

        # Coherence within RHS level-group: attrs must be jointly defined
        for i, a1 in enumerate(rhs_group):
            for a2 in rhs_group[i + 1 :]:
                if cross_level:
                    prefix = f"R2.{lhs_attrs[0]} IS NOT NULL AND "
                else:
                    prefix = ""
                for b in [
                    f"({prefix}R2.{a1} IS NOT NULL AND R2.{a2} IS NULL)",
                    f"({prefix}R2.{a1} IS NULL AND R2.{a2} IS NOT NULL)",
                ]:
                    if b not in branches:
                        branches.append(b)

        return branches

    @staticmethod
    def _build_containment_pruning(hierarchy: GuardHierarchy) -> list[dict]:
        """Build pruning rules to remove less-informative tuples after JOIN."""
        if len(hierarchy.levels) <= 1 or not hierarchy.nullable_cols:
            return []

        rules = []
        for i in range(len(hierarchy.levels) - 1):
            poorer = hierarchy.levels[i]
            richer = hierarchy.levels[i + 1]

            # Columns that distinguish richer from poorer
            new_not_null = [
                c for c in richer.not_null_cols if c not in poorer.not_null_cols
            ]
            if not new_not_null:
                continue

            richer_check = " AND ".join(
                f"{c} IS NOT NULL" for c in richer.not_null_cols
            )
            richer_condition = " AND ".join(
                f"t_rich.{c} IS NOT NULL" for c in richer.not_null_cols
            )
            poorer_condition = " AND ".join(f"t_poor.{c} IS NULL" for c in new_not_null)
            identity_match = " AND ".join(
                f"t_rich.{c} = t_poor.{c}"
                for c in (hierarchy.mandatory_cols or hierarchy.source_pk)
            )

            rules.append(
                {
                    "richer_check": richer_check,
                    "richer_condition": richer_condition,
                    "poorer_condition": poorer_condition,
                    "identity_match": identity_match,
                }
            )

        return rules

    @staticmethod
    def _build_null_pattern_where(hierarchy: GuardHierarchy) -> str:
        """Build WHERE clause with valid null-pattern disjunction."""
        parts = []

        # Identity columns always NOT NULL (mandatory, or source PK as fallback)
        id_cols = hierarchy.mandatory_cols or hierarchy.source_pk
        if id_cols:
            parts.append(" AND ".join(f"{c} IS NOT NULL" for c in id_cols))

        if not hierarchy.nullable_cols:
            return " AND ".join(parts) if parts else "TRUE"

        # Exclude identity columns from the disjunction
        pattern_nullable = [c for c in hierarchy.nullable_cols if c not in id_cols]

        if not pattern_nullable:
            return " AND ".join(parts) if parts else "TRUE"

        # Valid null-pattern branches (one per hierarchy level)
        branches = []
        for level in hierarchy.levels:
            branch_parts = []
            for col in pattern_nullable:
                if col in level.not_null_cols:
                    branch_parts.append(f"{col} IS NOT NULL")
                else:
                    branch_parts.append(f"{col} IS NULL")
            branches.append("(" + " AND ".join(branch_parts) + ")")

        pattern_clause = "(" + " OR ".join(branches) + ")"
        parts.append(pattern_clause)

        return " AND ".join(parts)

    def _inc_sql(self, context: Context) -> str:
        """Generate trigger-based INC enforcement for intra-table inclusion dependencies."""
        parts = []
        idx = 0
        for inc in context.inclusion_subsumptions:
            names = list(inc.relation_names)
            if names[0] != names[1]:
                continue  # Only handle intra-table INC here; inter-table uses FKs
            idx += 1
            attrs = list(inc.attributes)
            mid = len(attrs) // 2
            if mid != 1:
                raise UnsupportedError(
                    f"Multi-column intra-table INC not supported: {attrs}"
                )
            referencing_col = attrs[0]
            referenced_col = attrs[mid]
            pk = context.primary_keys.get(names[0], [])
            parts.append(
                self._render(
                    "inc_check.sql.j2",
                    table_name=names[0],
                    referencing_col=referencing_col,
                    referenced_col=referenced_col,
                    referenced_table=names[0],
                    self_ref_col=pk[0] if pk else referenced_col,
                    inc_index=idx,
                )
            )
        return "\n\n".join(parts) if parts else ""

    def _fd_sql(self, context: Context) -> str:
        fds = context.functional_dependencies
        if not fds:
            return ""

        hierarchy = self._build_guard_hierarchy()
        parts = []
        for i, fd in enumerate(fds, 1):
            lhs_attrs = list(fd.attributes)[:-1]
            rhs_attrs = list(fd.attributes)[-1:]

            table = self._find_table(fd.relation_name, context)
            all_attrs = table.attributes
            new_cols = ", ".join(f"NEW.{a}" for a in all_attrs)

            # Extract guard attributes if FD is guarded (child is SelectNode)
            guard_attrs = []
            if isinstance(fd.child, SelectNode):
                guard_attrs = self._extract_defined_attrs(fd.child.conditions)

            if guard_attrs:
                # Guarded FD -> CFD template with exhaustive OR branches
                where_branches = self._build_cfd_where_branches(
                    lhs_attrs, rhs_attrs, guard_attrs, hierarchy
                )
                parts.append(
                    self._render(
                        "cfd_check.sql.j2",
                        table_name=fd.relation_name,
                        fd_index=i,
                        new_cols=new_cols,
                        lhs_attrs=lhs_attrs,
                        rhs_attrs=rhs_attrs,
                        where_branches=where_branches,
                    )
                )
            else:
                # Unguarded FD -> existing simple template
                lhs_condition = " AND ".join(f"r1.{a} = r2.{a}" for a in lhs_attrs)
                rhs_condition = " AND ".join(f"r1.{a} <> r2.{a}" for a in rhs_attrs)
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
                        guard_attrs=[],
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
            inc = self._inc_sql(context)
            if inc:
                parts.append(inc)
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
                "guard_check": " AND ".join(
                    f"{a} IS NOT NULL" for a in self._extract_table_guard_attrs(t)
                ),
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

        # Per-table WHERE: source PK columns + each target table's PK columns
        src_pk_cols: list[str] = []
        for t in source.tables:
            for col in source.primary_keys.get(t.name, []):
                if col not in src_pk_cols:
                    src_pk_cols.append(col)
        for info in tgt_tables_info:
            where_cols = list(src_pk_cols)
            for pk_col in info["pk"]:
                if pk_col not in where_cols:
                    where_cols.append(pk_col)
            info["where_not_null"] = " AND ".join(
                f"{a} IS NOT NULL" for a in where_cols
            )
        hierarchy = self._build_guard_hierarchy()
        tgt_insert_where = self._build_null_pattern_where(hierarchy)

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
        prune_rules = self._build_containment_pruning(hierarchy)
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
                use_abs=False,
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

    def _build_target_delete_checks(self, source: Context) -> list[dict]:
        """Build independence checks for target→source DELETE mapping.

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
