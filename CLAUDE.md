# CLAUDE.md

## Project Overview

SSTC (Semantic SQL Transducer Compiler) compiles relational algebra definitions into PostgreSQL SQL. It parses source/target context definitions (relational algebra) against a universal schema (JSON) using RAPT2, then generates CREATE statements, INSERT tracking tables, trigger functions, and universal mappings.

## Commands

Requires Python >= 3.13.

```bash
uv sync                                         # Install dependencies
uv sync --group dev                             # Install dev dependencies (pytest, ruff, debugpy)
uv run pytest                                   # Run all tests
uv run pytest test/test_context.py::test_name   # Run single test
uv run ruff check .                             # Lint
uv run ruff format .                            # Format
```

## Architecture

**Data flow:** Universal JSON + relational algebra text files → RAPT2 parser → Context/Table objects → SQL output

### Core pipeline (`src/sstc/`)

- **`context.py`** — `Context.from_file(universal_path, context_path)` parses relational algebra via `Rapt(grammar="Dependency Grammar")`, separating results into `AssignNode`s (table definitions), `DependencyNode`s (constraints), and a special `UniversalMapping` assignment. Builds `Table` instances and a RAPT2 `Schema`.
- **`table.py`** — `Table` wraps an `AssignNode` with its associated dependency nodes. Generates SQL via `gen_concrete_create_stmt()` (typed columns from universal schema), `gen_universal_create_stmt()` (via RAPT2's `sql_translator.translate()`), and insert tracking infrastructure (`gen_insert_table_create`, `gen_insert_function`, `gen_insert_trigger`).
- **`definition.py`** — `AttributeSchema` and `TableSchema` are `DataClassJsonMixin` dataclasses for JSON deserialization (used by `context.py` and `table.py`). Also defines `TargetDefinition`/`SourceDefinition` abstractions, but these are currently unused by the pipeline.
- **`transducer_context.py`** — `TransducerContext` holds source and target `Context` instances, created via `from_files()`.
- **`transducer.py`** — `Transducer` entry point; `compile()` is not yet implemented.
- **`__init__.py`** — Public API exports: `Context`, `Transducer`, `TransducerContext`.
- **`__main__.py`** — CLI entry point (stub — just prints "Hello, World!"). Mapped to `sstc` command via `pyproject.toml` scripts.

### Key patterns

- Factory class methods (`from_file`, `from_relations_and_dependencies`) create instances from parsed external data
- RAPT2 node types: `AssignNode` (table definitions), `UnaryDependencyNode`/`BinaryDependencyNode` (constraints like PK, FD, INC, MVD)
- Table names in input use plain names (e.g. `Person_Source`, `PersonPhone`); RAPT2 lowercases them
- The reserved name `UniversalMapping` in relational algebra files defines the universal-to-context mapping

### Key dependency

`rapt2` is installed as an editable dependency from sibling directory `../rapt2`. It must be present for the project to build.

### Gotchas

- Generated insert functions reference a `_loop` table (for cycle detection) — this table must exist in the target database
- `gen_universal_create_stmt()` uses `use_bag_semantics=True` and `.replace("TEMPORARY TABLE", "TABLE")` — workaround because RAPT2's translator generates `CREATE TEMPORARY TABLE` by default
- Tests use `test/fixtures.py` with direct import, not `conftest.py`
- Tests must be run from the project root (fixture paths are relative)

## Input format

- **Universal schema**: JSON array of `{name, data_type, is_nullable}` objects
- **Context definitions**: Relational algebra text files using RAPT2 syntax with operators like `\project_{}`, `\select_{}`, `\natural_join`, and constraint declarations (`pk_{}`, `fd_{}`, `mvd_{}`, `inc=_{}`, `inc⊆_{}`)

See `test/inputs/example1/` for working examples.

## Reference materials

- **`docs/notes/`** — Design documentation: architecture layers, constraint theory (FDs, MVDs, guards, CJDs), SQL generation strategy (insert/delete chains, mapping functions), and a worked example with reference SQL output
- **`docs/papers/`** — Research paper the compiler is based on
- **`notes/`** — Reference SQL scripts (`desired_output.sql`, `full_script_v5_tabletemp.sql`) showing the target output the compiler should produce
