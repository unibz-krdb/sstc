# SSTC Software Architecture

This document describes the internal architecture of the SSTC compiler: how the source code is organized, how data flows through the compilation pipeline, what design patterns are used, and where the key boundaries lie.

For the *transducer's* architecture (the three-layer table stack, trigger chains, and constraint theory that the compiler targets), see [notes/architecture/](notes/architecture/) and [notes/constraints/](notes/constraints/).

---

## Overview

SSTC compiles relational algebra definitions into a PostgreSQL SQL script that implements bidirectional schema synchronization. The compiler reads three inputs and produces one output:

```
universal.json  ─┐
source.txt      ─┼──▶  SSTC  ──▶  transducer.sql
target.txt      ─┘
```

- **universal.json** — JSON array describing the universal relation's columns (name, data type, nullability)
- **source.txt** — relational algebra defining the source schema (tables, constraints, mapping)
- **target.txt** — relational algebra defining the target schema (tables, constraints, mapping)
- **transducer.sql** — complete PostgreSQL DDL: schema, tables, foreign keys, constraint enforcement triggers, tracking infrastructure, join staging, and bidirectional mapping functions

---

## Module Map

All source code lives under `src/sstc/`. Modules are listed in dependency order (leaves first):

```
src/sstc/
├── definition.py          # AttributeSchema dataclass (JSON deserialization)
├── table.py               # Table wrapper over RAPT2 AssignNode
├── guard.py               # Guard hierarchy, null-pattern logic (pure functions)
├── context.py             # Direction enum, Context class (RAPT2 parsing entry)
├── constraints.py         # MVD/FD/CFD/INC SQL generation (pure functions + RenderFn)
├── transducer_context.py  # Pairs source + target Context
├── generator.py           # Compilation orchestrator (Jinja2 templates)
├── transducer.py          # Public entry point (wraps Generator)
├── __main__.py            # CLI entry point
├── __init__.py            # Public API exports
└── templates/             # 15 Jinja2 SQL templates
```

### Dependency Graph

```
definition.py          ◄── (leaf: no project imports)
       ▲
table.py               ◄── (leaf: only rapt2 imports)
       ▲
guard.py               ◄── definition, table
       ▲
context.py             ◄── definition, table
       ▲
constraints.py         ◄── context, guard, table
       ▲
transducer_context.py  ◄── context
       ▲
generator.py           ◄── constraints, context, guard, table, transducer_context
       ▲
transducer.py          ◄── generator, transducer_context
       ▲
__main__.py            ◄── generator, transducer_context
```

Arrows point from importer to dependency. There are no circular dependencies. The key design invariant is:

> **guard.py** (leaf) &larr; **constraints.py** &larr; **generator.py** (orchestrator)

`guard.py` and `constraints.py` contain pure functions with no Jinja2 knowledge. `generator.py` is the only module that touches templates.

---

## Data Flow

The compilation pipeline has four stages. Each stage transforms data and passes it to the next.

### Stage 1: Parsing (`Context.from_file`)

```
universal.json ──▶ json.load() ──▶ list[AttributeSchema]
                                          │
source.txt ──▶ RAPT2 parser ──▶ AssignNodes + DependencyNodes
                                          │
                                          ▼
                                   Context(tables, dependency_nodes,
                                           universal_schema, direction)
```

`Context.from_file()` is the sole RAPT2 integration point. It:

1. Deserializes the universal schema JSON into `list[AttributeSchema]`
2. Feeds the RA text through `Rapt(grammar="Dependency Grammar").to_syntax_tree()`
3. Separates the resulting node tree: `AssignNode`s become table definitions (or the reserved `UniversalMapping`), `DependencyNode`s become constraints
4. Calls `Table.from_relations_and_dependencies()` to group each table with its constraints

The output is a `Context` holding `tables`, `dependency_nodes`, `universal_schema`, and `direction` (SOURCE or TARGET).

### Stage 2: Pairing (`TransducerContext.from_files`)

```
Context(SOURCE) ─┐
                  ├──▶ TransducerContext(source, target)
Context(TARGET) ─┘
```

Thin wrapper that calls `Context.from_file()` twice with the same universal schema path, once per direction.

### Stage 3: Analysis (`guard.py`, `constraints.py`)

Before SQL generation, two analysis modules derive intermediate structures from the parsed data:

**guard.py** builds the *specialization hierarchy* — an ordering of target tables by their guard conditions (which nullable columns must be non-NULL). This hierarchy drives:
- CFD enforcement branch generation (`build_cfd_where_branches`)
- Containment pruning rules (`build_containment_pruning`)
- Valid null-pattern WHERE clauses (`build_null_pattern_where`)

**constraints.py** generates SQL for constraint enforcement:
- MVD checks and grounding functions (`mvd_sql`)
- FD/CFD trigger functions (`fd_sql`)
- Intra-table INC enforcement (`inc_sql`)
- Foreign keys from inclusion dependencies (`foreign_keys`)

Both modules are pure-functional: they accept data in, return strings out. `constraints.py` functions accept a `RenderFn` callback for template rendering (see [RenderFn Pattern](#renderfn-callback-pattern) below).

### Stage 4: Orchestration (`Generator.compile`)

```
TransducerContext
       │
       ▼
Generator.compile() ──▶ SQL string
       │
       ├── _preamble()        schema DROP/CREATE, _loop table
       ├── _base_tables()     CREATE TABLE for all source + target tables
       ├── _foreign_keys()    ALTER TABLE ADD FOREIGN KEY
       ├── _constraints()     MVD/FD/CFD/INC trigger functions
       ├── _tracking()        INSERT/DELETE tracking tables + capture triggers
       ├── _join()            JOIN staging tables + natural-join functions
       └── _mapping()         4 mapping functions + triggers
                              (SOURCE_INSERT_FN, TARGET_INSERT_FN,
                               SOURCE_DELETE_FN, TARGET_DELETE_FN)
```

`Generator` owns the Jinja2 environment and the `_render()` method. Each pipeline method either renders templates directly or delegates to `constraints.py` / `guard.py` for the data, then renders. The seven sections are joined with `"\n\n"` into the final SQL string.

---

## Key Data Types

### `AttributeSchema` (`definition.py`)

```python
@dataclass
class AttributeSchema(DataClassJsonMixin):
    name: str
    data_type: str
    is_nullable: bool
```

Represents one column from the universal schema JSON. Used to determine column types for `CREATE TABLE` and nullability for the guard hierarchy. Created by `Context.from_file()`, consumed by `Generator._table_columns()` and `guard.build_guard_hierarchy()`.

### `Table` (`table.py`)

```python
class Table:
    definition: AssignNode       # RAPT2 AST node (table's RA expression)
    dependency_nodes: list[DependencyNode]  # Constraints scoped to this table
```

Wraps a RAPT2 `AssignNode` with its associated dependency constraints. Exposes `name` and `attributes` as convenience properties. The `definition` field provides access to the full RAPT2 AST for guard extraction (`guard.py` traverses `table.definition.child` to find `SelectNode` conditions).

Created by the `from_relations_and_dependencies` factory method, which matches each `AssignNode` to the dependency nodes that reference it.

### `Context` (`context.py`)

```python
class Context:
    tables: list[Table]
    direction: Direction           # SOURCE or TARGET
    dependency_nodes: list[DependencyNode]
    universal_schema: list[AttributeSchema]
    universal_mapping: AssignNode | None
```

The central data container for one side of the transduction. Created by `from_file()`, which is the RAPT2 parsing entry point. Exposes typed-filter properties for each constraint kind:

- `primary_keys` &rarr; `dict[str, list[str]]`
- `functional_dependencies` &rarr; `list[FunctionalDependencyNode]`
- `multivalued_dependencies` &rarr; `list[MultivaluedDependencyNode]`
- `inclusion_equivalences` &rarr; `list[InclusionEquivalenceNode]`
- `inclusion_subsumptions` &rarr; `list[InclusionSubsumptionNode]`

### `Direction` (`context.py`)

```python
class Direction(enum.StrEnum):
    SOURCE = "source"
    TARGET = "target"
```

Uses `enum.StrEnum` (not `str, enum.Enum`) so that values render correctly as plain strings in Jinja2 templates.

### `GuardHierarchy` / `GuardLevel` (`guard.py`)

```python
@dataclass
class GuardLevel:
    guard_attrs: set[str]        # Nullable columns that must be NOT NULL at this level
    tables: list[str]            # Target tables belonging to this level
    not_null_cols: list[str]     # Cumulative NOT NULL columns up to this level
    null_cols: list[str]         # Remaining nullable columns at this level

@dataclass
class GuardHierarchy:
    mandatory_cols: list[str]    # Always-NOT-NULL columns from universal schema
    nullable_cols: list[str]     # Nullable columns from universal schema
    levels: list[GuardLevel]     # Ordered by increasing guard specificity
    source_pk: list[str]         # Source primary key columns
```

The specialization hierarchy orders target tables by how many nullable columns they require to be non-NULL. Level 0 has no guard (empty `guard_attrs`); each successive level adds more NOT NULL requirements. This hierarchy is used to generate CFD enforcement branches, containment pruning, and null-pattern WHERE clauses.

### `UnsupportedError` (`constraints.py`)

```python
class UnsupportedError(Exception):
    """Raised when the generator encounters a constraint pattern it cannot compile."""
```

Raised for input patterns the compiler cannot yet handle: multiple source tables, non-shared-LHS MVDs, multi-column intra-table INCs.

---

## Design Patterns

### Factory Class Methods

All domain classes use `@classmethod` factory methods for construction from external data:

| Class | Factory | Input |
|---|---|---|
| `Context` | `from_file()` | file paths + direction |
| `Table` | `from_relations_and_dependencies()` | RAPT2 node lists |
| `TransducerContext` | `from_files()` | file paths |
| `Transducer` | `from_file()` | file paths |

`__init__` methods accept already-parsed data. Factory methods handle parsing and grouping.

### RenderFn Callback Pattern

`constraints.py` generates SQL without depending on Jinja2. Functions accept a callback:

```python
RenderFn = Callable[..., str]

def mvd_sql(context: Context, render: RenderFn) -> str:
    ...
    render("mvd_check.sql.j2", table_name=name, select_cols=cols, ...)
```

`Generator` passes `self._render` as the callback. This decouples constraint logic from template rendering: `constraints.py` knows template *names* and *variable names*, but not how rendering works. This enables testing constraint logic with a mock render function.

The pattern is used consistently by all public functions in `constraints.py`: `mvd_sql`, `fd_sql`, `inc_sql`, and `constraints`.

### Pure Functions in Leaf Modules

`guard.py` and `constraints.py` are composed of pure functions that accept data and return data (or SQL strings). They have no mutable state, no class instances, and no side effects. This makes them independently testable and easy to reason about.

`guard.py` functions:
- `extract_defined_attrs(cond)` &rarr; `list[str]`
- `extract_table_guard_attrs(table)` &rarr; `list[str]`
- `build_guard_hierarchy(tables, schema, pks)` &rarr; `GuardHierarchy`
- `build_cfd_where_branches(lhs, rhs, guards, hierarchy)` &rarr; `list[str]`
- `build_containment_pruning(hierarchy)` &rarr; `list[dict]`
- `build_null_pattern_where(hierarchy)` &rarr; `str`

`constraints.py` functions:
- `foreign_keys(source, target, schema)` &rarr; `str`
- `mvd_sql(context, render)` &rarr; `str`
- `fd_sql(context, hierarchy, render)` &rarr; `str`
- `inc_sql(context, render)` &rarr; `str`
- `constraints(source, target, hierarchy, render)` &rarr; `str`

---

## Template Layer

15 Jinja2 templates live in `src/sstc/templates/`. They are loaded by `Generator.__init__` via `jinja2.FileSystemLoader` and rendered exclusively through `Generator._render()`.

| Template | Purpose | Complexity |
|---|---|---|
| `preamble.sql.j2` | Schema DROP/CREATE, `_loop` table | Trivial |
| `create_table.sql.j2` | CREATE TABLE with columns + PK | Low |
| `tracking_table.sql.j2` | Shadow clone for INSERT/DELETE tracking | Trivial |
| `capture_function.sql.j2` | Capture trigger function (loop-guarded) | Low |
| `capture_trigger.sql.j2` | AFTER INSERT/DELETE trigger wiring | Trivial |
| `join_function.sql.j2` | Natural-join staging + loop sentinel | Medium |
| `join_trigger.sql.j2` | Trigger wiring for join staging | Trivial |
| `mapping_trigger.sql.j2` | Trigger wiring for mapping functions | Trivial |
| `insert_mapping.sql.j2` | SOURCE/TARGET INSERT mapping function | High |
| `delete_mapping.sql.j2` | SOURCE/TARGET DELETE mapping function | High |
| `mvd_check.sql.j2` | MVD violation check trigger | Medium |
| `mvd_grounding.sql.j2` | MVD grounding (complementary tuple insert) | Medium |
| `fd_check.sql.j2` | Unguarded FD enforcement trigger | Medium |
| `cfd_check.sql.j2` | Guarded FD (CFD) enforcement trigger | Medium |
| `inc_check.sql.j2` | Intra-table INC enforcement trigger | Medium |

Templates receive pre-computed Python data (dicts, lists, strings). All conditional logic (guard checks, pruning rules, independence checks) is computed in Python and passed as template variables. Templates contain no business logic beyond structural conditionals (`{% if %}`, `{% for %}`).

Jinja2 is fully contained within `generator.py`. No other module imports or references Jinja2.

---

## RAPT2 Boundary

[RAPT2](../rapt2) is the relational algebra parser, installed as an editable dependency from the sibling `../rapt2` directory.

### Where RAPT2 types appear

| Module | RAPT2 types used |
|---|---|
| `table.py` | `AssignNode`, `DependencyNode`, `UnaryDependencyNode`, `BinaryDependencyNode` |
| `context.py` | `Rapt`, `AssignNode`, `DependencyNode`, 5 dependency subtypes, `Schema` |
| `guard.py` | `SelectNode`, `UnaryNode`, `BinaryConditionNode`, `UnaryConditionNode`, `UnaryConditionalOperator` |
| `constraints.py` | `SelectNode` (for `isinstance` check in `fd_sql`) |

### Where RAPT2 types do NOT appear

`generator.py`, `transducer_context.py`, `transducer.py`, `__main__.py`, and `__init__.py` have no RAPT2 imports. The boundary is partially contained: RAPT2 is allowed in the data-model layer (`table`, `context`) and the pure-logic layer (`guard`, `constraints`), but blocked from the orchestration layer (`generator`) and entry points.

`Context` properties like `functional_dependencies` return typed RAPT2 node lists, so consumers (`constraints.py`, `guard.py`) must understand RAPT2 node attributes to traverse them.

---

## Compilation Pipeline Sections

The seven sections produced by `Generator.compile()`, in order:

### 1. Preamble

Drops and recreates the `transducer` schema. Creates the `_loop` table used for cycle detection (see [loop-prevention.md](notes/architecture/loop-prevention.md)).

### 2. Base Tables

`CREATE TABLE` for every table in both source and target contexts. Column types and nullability come from the universal schema JSON. Primary keys are derived from `pk_{}` declarations in the RA.

### 3. Foreign Keys

`ALTER TABLE ADD FOREIGN KEY` statements derived from inclusion dependencies (`inc=_{}` and `inc⊆_{}`). An inclusion whose referenced columns match the referenced table's primary key becomes a foreign key constraint.

### 4. Constraints

Trigger-based enforcement functions for three constraint types:

- **MVDs** — Check function rejects inserts that violate multivalued dependencies. Grounding function inserts complementary tuples to restore 4NF.
- **FDs/CFDs** — For unguarded FDs, a check function ensures LHS-match implies RHS-match. For guarded FDs (conditional FDs), the guard hierarchy generates exhaustive OR-branches covering all null-pattern states that would violate the dependency.
- **INCs** — Intra-table inclusion dependencies enforced by check triggers.

See [notes/constraints/](notes/constraints/) for the theory behind each constraint type.

### 5. Tracking

For each base table, per direction, per event (INSERT/DELETE):
- A tracking table (shadow clone of the base table's columns)
- A capture function (copies NEW/OLD row into the tracking table, guarded by the `_loop` table)
- An AFTER trigger wiring the capture function to the base table

### 6. Join Staging

For each base table, per direction, per event:
- A JOIN staging table (tracks the natural-join result)
- A function that natural-left-outer-joins the tracking table with all other base tables, projects into per-table staging tables, and inserts a sentinel into `_loop`
- A trigger wiring the join function to the tracking table

See [notes/architecture/layers.md](notes/architecture/layers.md) for why the join layer exists (the partial updates problem).

### 7. Mapping

Four bidirectional mapping functions:

| Function | Reads from | Writes to | Special logic |
|---|---|---|---|
| `SOURCE_INSERT_FN` | Source JOIN staging | Target base tables | Direct projection (single source table) |
| `TARGET_INSERT_FN` | Target JOIN staging | Source base tables | Containment pruning, null-pattern WHERE |
| `SOURCE_DELETE_FN` | Source JOIN staging | Target base tables | MVD independence checks |
| `TARGET_DELETE_FN` | Target JOIN staging | Source base tables | Full tuple independence checks |

Each function is wired to the JOIN staging tables via mapping triggers. After executing, each function truncates its tracking and staging tables.

---

## Loop Prevention

The `_loop` table with a single `loop_start` column prevents infinite trigger recursion. When source triggers fire, they insert a sentinel value (+1); target capture functions check for this value and suppress if found (and vice versa with -1). Constants in `generator.py`:

```python
SOURCE_LOOP_CHECK = -1    # Source capture checks for target's sentinel
TARGET_LOOP_CHECK = 1     # Target capture checks for source's sentinel
SOURCE_LOOP_VALUE = 1     # Source join functions insert this
TARGET_LOOP_VALUE = -1    # Target join functions insert this
```

The relationship: `LOOP_VALUE` and `LOOP_CHECK` are negations. Source inserts +1, target checks for +1; target inserts -1, source checks for -1.

See [notes/architecture/loop-prevention.md](notes/architecture/loop-prevention.md) for details.

---

## Entry Points

### CLI (`__main__.py`)

```bash
sstc universal.json source.txt target.txt [-o output.sql]
```

Mapped to the `sstc` command via `pyproject.toml` scripts. Constructs a `TransducerContext` and calls `Generator.compile()` directly.

### Library (`Transducer`)

```python
from sstc import Transducer

t = Transducer.from_file("universal.json", "source.txt", "target.txt")
sql = t.compile()
```

`Transducer` is the public API entry point exported from `__init__.py`. It wraps `TransducerContext` + `Generator`.

### Public API (`__init__.py`)

Exports: `Context`, `Direction`, `Transducer`, `TransducerContext`. `Generator` is intentionally excluded — callers should use `Transducer.compile()`.

---

## Current Limitations

- **Single source table only.** `Generator.compile()` raises `UnsupportedError` if the source context has more than one table. The compiler currently targets Universal Relation Assumption (URA) schemas where the source side is a single universal table decomposed into multiple target tables.
- **Shared-LHS MVDs only.** All MVDs on a table must share the same LHS determinant.
- **Single-column intra-table INCs only.** Multi-column intra-table inclusion dependencies are not supported.
- **No schema validation at the boundary.** Attributes in the RA that are absent from the universal schema JSON are silently skipped rather than raising an error.
