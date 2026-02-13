# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SSTC (Semantic SQL Transducer Compiler) is a Python prototype that compiles relational algebra definitions into SQL. It manages schema transformations and constraints across source, target, and universal databases. The project uses RAPT2 (relational algebra parser) to parse definitions and generates PostgreSQL-compatible SQL.

## Commands

```bash
# Install dependencies (requires uv package manager)
uv sync

# Install dev dependencies (pytest, ruff, debugpy)
uv sync --group dev

# Run tests
uv run pytest

# Run single test
uv run pytest test/test_context.py::test_name

# Lint and format
uv run ruff check .
uv run ruff format .
```

## Architecture

**Data flow:** JSON schema + relational algebra files → Parse (Context) → Extract Tables → Generate SQL

### Core Modules (src/sstc/)

- **context.py**: `Context` class parses relational algebra from files using RAPT2, builds schema and tables for source/target databases
- **table.py**: `Table` class represents source/target tables, generates SQL create statements, insert functions, triggers, and universal mappings
- **definition.py**: Data classes (`Definition`, `TargetDefinition`, `SourceDefinition`, `AttributeSchema`, `TableSchema`) for schema representation
- **transducer_context.py**: `TransducerContext` orchestrates source and target contexts together
- **transducer.py**: `Transducer` main class (compile() not yet implemented)

### Key Dependencies

- `rapt2`: Relational algebra parser (installed as editable from sibling directory `../rapt2`)
- `dataclasses-json`: JSON serialization for dataclasses

## Input Format

The system uses:
- **Universal schema**: JSON file with attribute definitions
- **Source/Target definitions**: Relational algebra text files parsed by RAPT2

Table names must be fully qualified: `{schema}.{tablename}`

See `test/inputs/example1/` for example input files.
