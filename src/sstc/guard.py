"""Guard hierarchy and null-pattern logic for the SSTC pipeline.

Derives the specialization hierarchy from universal schema nullability
and target table guards. Provides pure functions for building CFD
enforcement branches, containment pruning rules, and null-pattern
WHERE clauses used by constraint enforcement and mapping generation.
"""

from dataclasses import dataclass, field

from rapt2.treebrd.condition_node import (
    BinaryConditionNode,
    UnaryConditionNode,
    UnaryConditionalOperator,
)
from rapt2.treebrd.node import SelectNode, UnaryNode

from .definition import AttributeSchema
from .table import Table


@dataclass
class GuardLevel:
    """A single level in the guard specialization hierarchy.

    Each level corresponds to a set of guard attributes (nullable columns
    that must be non-NULL for certain target tables to be populated).
    Tracks which tables belong to this level and the cumulative null/not-null
    partition of nullable columns up to this point.
    """

    guard_attrs: set[str]
    tables: list[str] = field(default_factory=list)
    not_null_cols: list[str] = field(default_factory=list)
    null_cols: list[str] = field(default_factory=list)


@dataclass
class GuardHierarchy:
    """The full specialization hierarchy derived from universal schema nullability and target guards.

    Organizes target tables into levels ordered by increasing guard
    specificity (more NOT NULL requirements). Used to generate CFD
    enforcement branches, containment pruning rules, and valid
    null-pattern WHERE clauses in mapping functions.
    """

    mandatory_cols: list[str]
    nullable_cols: list[str]
    levels: list[GuardLevel]
    source_pk: list[str]


def extract_defined_attrs(cond) -> list[str]:
    """Recursively extract attribute names from DEFINED(...) conditions in a condition tree."""
    if isinstance(cond, UnaryConditionNode):
        if cond.op == UnaryConditionalOperator.DEFINED:
            return cond.child.attribute_references()
        return []
    if isinstance(cond, BinaryConditionNode):
        return extract_defined_attrs(cond.left) + extract_defined_attrs(cond.right)
    return []


def extract_table_guard_attrs(table: Table) -> list[str]:
    """Extract guard attributes from a target table's select clause."""
    node = table.definition.child  # Skip AssignNode -> get ProjectNode
    while node is not None:
        if isinstance(node, SelectNode):
            return extract_defined_attrs(node.conditions)
        if isinstance(node, UnaryNode):
            node = node.child
        else:
            return []
    return []


def build_guard_hierarchy(
    target_tables: list[Table],
    universal_schema: list[AttributeSchema],
    source_primary_keys: dict[str, list[str]],
) -> GuardHierarchy:
    """Build the specialization hierarchy from universal schema + target table guards."""
    mandatory_cols = [a.name for a in universal_schema if not a.is_nullable]
    nullable_cols = [a.name for a in universal_schema if a.is_nullable]

    # Extract distinct guard sets from target tables
    guard_sets: dict[frozenset[str], list[str]] = {}
    for table in target_tables:
        guard = frozenset(extract_table_guard_attrs(table))
        guard_sets.setdefault(guard, []).append(table.name)

    # Always include empty guard (Level 0)
    if frozenset() not in guard_sets:
        guard_sets[frozenset()] = []

    # Sort by cardinality ascending
    sorted_guards = sorted(guard_sets.items(), key=lambda x: len(x[0]))

    # Build levels with cumulative not_null / null columns
    levels = []
    cumulative: set[str] = set()
    for guard_frozen, tables in sorted_guards:
        cumulative |= set(guard_frozen)

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
    for pk_cols in source_primary_keys.values():
        for col in pk_cols:
            if col not in src_pk:
                src_pk.append(col)

    return GuardHierarchy(
        mandatory_cols=mandatory_cols,
        nullable_cols=nullable_cols,
        levels=levels,
        source_pk=src_pk,
    )


def build_cfd_where_branches(
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

    # Branch 1: Main FD violation -- all guards non-NULL, LHS match, RHS differ
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

    def find_group(attr: str) -> tuple[int, list[str]]:
        for i, group in enumerate(level_groups):
            if attr in group:
                return i, group
        return -1, [attr]

    rhs_idx, rhs_group = find_group(rhs_attrs[0])
    lhs_idx, _ = find_group(lhs_attrs[0])
    cross_level = lhs_idx != rhs_idx

    # Cross-level: LHS NULL -> no RHS-group attr can be NOT NULL
    if cross_level:
        for lhs_attr in lhs_attrs:
            for rhs_attr in rhs_group:
                branch = f"(R2.{lhs_attr} IS NULL AND R2.{rhs_attr} IS NOT NULL)"
                if branch not in branches:
                    branches.append(branch)

    # Coherence within RHS level-group: attrs must be jointly defined
    for i, attr1 in enumerate(rhs_group):
        for attr2 in rhs_group[i + 1 :]:
            if cross_level:
                prefix = f"R2.{lhs_attrs[0]} IS NOT NULL AND "
            else:
                prefix = ""
            for branch in [
                f"({prefix}R2.{attr1} IS NOT NULL AND R2.{attr2} IS NULL)",
                f"({prefix}R2.{attr1} IS NULL AND R2.{attr2} IS NOT NULL)",
            ]:
                if branch not in branches:
                    branches.append(branch)

    return branches


def build_containment_pruning(hierarchy: GuardHierarchy) -> list[dict]:
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

        richer_check = " AND ".join(f"{c} IS NOT NULL" for c in richer.not_null_cols)
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


def build_null_pattern_where(hierarchy: GuardHierarchy) -> str:
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

    # Columns in any guard set vary by level; the rest are always NOT NULL
    all_guard_attrs: set[str] = set()
    for level in hierarchy.levels:
        all_guard_attrs |= level.guard_attrs

    non_guard = [c for c in pattern_nullable if c not in all_guard_attrs]
    guard_cols = [c for c in pattern_nullable if c in all_guard_attrs]

    if non_guard:
        parts.extend(f"{c} IS NOT NULL" for c in non_guard)

    # Valid null-pattern branches (one per hierarchy level)
    if guard_cols:
        branches = []
        for level in hierarchy.levels:
            branch_parts = []
            for col in guard_cols:
                if col in level.not_null_cols:
                    branch_parts.append(f"{col} IS NOT NULL")
                else:
                    branch_parts.append(f"{col} IS NULL")
            branches.append("(" + " AND ".join(branch_parts) + ")")

        pattern_clause = "(" + " OR ".join(branches) + ")"
        parts.append(pattern_clause)

    return " AND ".join(parts)
