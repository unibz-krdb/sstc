# Integration Test Design: PostgreSQL End-to-End Verification

## Problem

The SSTC compiler generates ~1,950 lines of PL/pgSQL (triggers, constraint-checking functions, insert/delete chains) but correctness is only verified at the string level via golden-file comparison. A generated trigger could have valid syntax yet wrong semantics, and no current test catches that.

## Solution

Automated integration tests that compile example inputs, execute the generated SQL against a real PostgreSQL instance via testcontainers, insert test data, and verify the transducer propagates correctly.

## Dependencies

Added to `dev` dependency group only:

- `testcontainers[postgres]` — spins up a throwaway Postgres Docker container
- `psycopg[binary]` — PostgreSQL driver (psycopg v3)

## Configuration

pytest marker in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = ["integration: requires Docker + PostgreSQL"]
```

- `uv run pytest` runs only unit tests (default)
- `uv run pytest -m integration` runs integration tests
- `uv run pytest -m "integration or not integration"` runs everything

## Fixture Architecture

All fixtures in `test/conftest.py`, guarded by `pytest.importorskip("testcontainers")`.

### Session-scoped container

One Postgres 17 container for the entire test run. Starts once, shared across all integration tests.

```python
@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:17", driver=None) as pg:
        yield pg
```

`driver=None` is required so `get_connection_url()` returns a plain `postgresql://` URL compatible with psycopg v3 (the default includes a psycopg2 driver prefix).

### Per-test connection

Each test gets a fresh connection. The compiled SQL starts with `DROP SCHEMA IF EXISTS transducer CASCADE; CREATE SCHEMA transducer;`, so every test naturally resets state.

```python
@pytest.fixture
def pg_conn(pg_container):
    conn = psycopg.connect(pg_container.get_connection_url())
    conn.autocommit = True
    yield conn
    conn.close()
```

### Compiled SQL fixture

Compiles example1 once per session, reuses the SQL string across tests.

```python
@pytest.fixture(scope="session")
def example1_sql():
    ctx = TransducerContext.from_files(
        universal_path="test/inputs/example1/universal.json",
        source_path="test/inputs/example1/source.txt",
        target_path="test/inputs/example1/target.txt",
    )
    return Generator(ctx).compile()
```

### Schema installer

Per-test fixture that executes the compiled SQL and returns the connection with a fully installed transducer.

```python
@pytest.fixture
def transducer_db(pg_conn, example1_sql):
    pg_conn.execute(example1_sql)
    return pg_conn
```

## Test Cases — Phase B (Source-to-Target Propagation)

File: `test/test_integration.py`. All tests marked `@pytest.mark.integration`.

### test_schema_installs

Smoke test. Execute compiled SQL, query `information_schema.tables` to verify all 9 base tables + `_loop` are present in the `transducer` schema.

### test_simple_person_propagates

Insert a minimal person (empid/hdate/dept/manager NULL) into `_person_source`. Verify:
- `_person` gets `(ssn, name)`
- `_personphone` gets `(ssn, phone)`
- `_personemail` gets `(ssn, email)`
- Employee-level tables remain empty

Tests guard hierarchy level 0: unguarded tables populated, guarded tables skipped.

### test_employee_propagates

Insert an employee tuple (empid + hdate non-null, dept/manager NULL). Verify:
- Level-0 tables populated (person, phone, email)
- Level-1 tables populated (`_employee`, `_employeedate`)
- Level-2 tables remain empty (`_ped`, `_peddept`, `_deptmanager`)

Tests guard hierarchy level 1.

### test_full_employee_with_dept_propagates

Insert a full tuple (all 8 attributes non-null). Verify all 8 target tables are populated with correct values.

Tests guard hierarchy level 2.

### test_mvd_grounding

Insert two source rows for the same `ssn` with different phone/email values. Verify that MVD grounding triggers generate cross-product tuples in `_personphone` and `_personemail` (4 rows each rather than 2).

## Future Extension Path

### Phase C — Bidirectional (target-to-source)

- Insert into target tables wrapped in `_loop` transactions
- Verify rows propagate back to `_person_source` with correct null patterns
- Test containment pruning (most-informative tuple wins)

### Phase D — Constraint enforcement

- `test_mvd_violation_rejected` — violate `mvd_{ssn, phone}`, assert `RAISE EXCEPTION`
- `test_cfd_violation_rejected` — e.g. empid non-null with hdate null, assert rejection
- `test_inc_violation_rejected` — manager referencing non-existent empid, assert rejection
- Use `pytest.raises(psycopg.errors.RaiseException)` for PL/pgSQL exceptions

Each phase adds tests to `test_integration.py` using the same fixture infrastructure.

## Decision Log

| Decision | Choice | Rationale |
|---|---|---|
| Database management | testcontainers | Zero manual setup, throwaway containers |
| Driver | psycopg v3 (raw SQL) | Project generates SQL; tests should speak SQL |
| Container lifecycle | Session-scoped | One startup cost; schema reset provides test isolation |
| Test isolation | Per-test schema reset | Compiled SQL already does `DROP SCHEMA CASCADE` |
| Test gating | `@pytest.mark.integration` | Keeps default `uv run pytest` fast |
| Initial scope | Phase B | Proves SQL is executable + semantically correct for primary use case |
