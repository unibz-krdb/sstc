# Guard Hierarchy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Patch 7 gaps between the current SSTC compiler and the new reference example with mixed nullability, enabling the compiler to handle nullable Universal Tables, CFDs, conditional INSERTs, and tuple containment.

**Architecture:** Introduce a `GuardHierarchy` abstraction computed once from the universal schema + target table guards. This single data structure drives conditional INSERTs (Gap 1), null-pattern WHERE clauses (Gap 2), tuple containment pruning (Gap 3), and CFD exhaustive checks (Gap 4). INC trigger enforcement (Gap 6) is handled independently. Gaps 5 and 7 already work.

**Tech Stack:** Python 3.13, Jinja2 templates, RAPT2 parser, pytest

**Design doc:** `docs/plans/2026-04-02-guard-hierarchy-design.md`

**Reference SQL:** `docs/notes/example/` (files 1-6 + `null_example_notes.sql`)

---

## Key Context for the Implementer

- The RAPT2 AST for a guarded target table like `Employee := \project_{ssn, empid} \select_{defined(empid) and defined(hdate)} Universal;` is: `AssignNode → ProjectNode → SelectNode(conditions) → RelationNode`. Unguarded tables skip the SelectNode.
- `Generator._extract_defined_attrs(cond)` at `src/sstc/generator.py:200-210` already extracts `defined()` attribute names from a `ConditionNode` tree. Reuse it.
- `UnaryNode` (at `rapt2/src/rapt2/treebrd/node.py:70`) is the base class for `ProjectNode`, `SelectNode`, `AssignNode`. Use it for traversal.
- The existing example1 tests must continue to pass. Some counts will change when INC triggers are added (+1 function, +1 trigger).
- All FDs in example1 are guarded, so when we switch guarded FDs to the new CFD template, the naming changes from `fd_N` to `cfd_N`. Tests must be updated accordingly.

---

### Task 1: Create Example 2 Test Inputs

**Files:**
- Create: `test/inputs/example2/universal.json`
- Create: `test/inputs/example2/source.txt`
- Create: `test/inputs/example2/target.txt`
- Modify: `test/fixtures.py`

**Step 1: Create the directory and universal schema**

```bash
mkdir -p test/inputs/example2
```

Write `test/inputs/example2/universal.json`:
```json
[
    {
        "name": "ssn",
        "data_type": "VARCHAR(100)",
        "is_nullable": false
    },
    {
        "name": "empid",
        "data_type": "VARCHAR(100)",
        "is_nullable": true
    },
    {
        "name": "name",
        "data_type": "VARCHAR(100)",
        "is_nullable": false
    },
    {
        "name": "hdate",
        "data_type": "VARCHAR(100)",
        "is_nullable": true
    },
    {
        "name": "phone",
        "data_type": "VARCHAR(100)",
        "is_nullable": false
    },
    {
        "name": "email",
        "data_type": "VARCHAR(100)",
        "is_nullable": false
    },
    {
        "name": "dept",
        "data_type": "VARCHAR(100)",
        "is_nullable": true
    },
    {
        "name": "manager",
        "data_type": "VARCHAR(100)",
        "is_nullable": true
    }
]
```

**Step 2: Create the source context**

Write `test/inputs/example2/source.txt`:
```
Person_Source := \project_{ssn, empid, name, hdate, phone, email, dept, manager} Universal;
pk_{ssn, phone, email} Person_Source;
mvd_{ssn, phone} Person_Source;
mvd_{ssn, email} Person_Source;
fd_{empid, hdate} \select_{defined(empid) and defined(hdate)} Person_Source;
fd_{empid, dept} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Person_Source;
fd_{dept, manager} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Person_Source;
inc⊆_{manager, ssn} (Person_Source, Person_Source);

UniversalMapping := \project_{ssn, empid, name, hdate, phone, email, dept, manager} Person_Source;
```

**Step 3: Create the target context**

Write `test/inputs/example2/target.txt`:
```
P := \project_{ssn, name} Universal;
pk_{ssn} P;

PersonPhone := \project_{ssn, phone} Universal;
pk_{ssn, phone} PersonPhone;

PersonEmail := \project_{ssn, email} Universal;
pk_{ssn, email} PersonEmail;

PE := \project_{ssn, empid} \select_{defined(empid) and defined(hdate)} Universal;
pk_{empid} PE;

PE_HDATE := \project_{empid, hdate} \select_{defined(empid) and defined(hdate)} Universal;
pk_{empid} PE_HDATE;

PED := \project_{ssn, empid} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Universal;
pk_{empid} PED;

PEDDept := \project_{empid, dept} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Universal;
pk_{empid} PEDDept;

DeptManager := \project_{dept, manager} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Universal;
pk_{dept} DeptManager;

inc=_{ssn, ssn} (P, PersonPhone);
inc=_{ssn, ssn} (P, PersonEmail);
inc=_{empid, empid} (PE, PE_HDATE);
inc=_{empid, empid} (PED, PEDDept);
inc=_{dept, dept} (PEDDept, DeptManager);

inc⊆_{ssn, ssn} (PE, P);
inc⊆_{empid, empid} (PED, PE);
inc⊆_{manager, ssn} (DeptManager, P);

UniversalMapping := \project_{ssn, empid, name, hdate, phone, email, dept, manager} (P \natural_join PersonPhone \natural_join PersonEmail \natural_join PE \natural_join PE_HDATE \natural_join PED \natural_join PEDDept \natural_join DeptManager);
```

**Step 4: Add fixture for example2**

Add to `test/fixtures.py`:
```python
@pytest.fixture
def example_2_dir():
    path = os.path.join("test", "inputs", "example2")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist.")
    return path
```

**Step 5: Smoke test — verify RAPT2 parses example2**

Add to `test/test_generator.py` (at bottom):
```python
from fixtures import example_2_dir as example_2_dir


def test_example2_parses(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    assert len(ctx.source.tables) == 1
    assert len(ctx.target.tables) == 8
```

**Step 6: Run tests**

Run: `uv run pytest test/test_generator.py::test_example2_parses -v`
Expected: PASS

**Step 7: Commit**

```bash
git add test/inputs/example2/ test/fixtures.py test/test_generator.py
git commit -m "test: add example2 inputs with mixed nullability and composite PK"
```

---

### Task 2: GuardHierarchy Abstraction

**Files:**
- Modify: `src/sstc/generator.py:1-34` (imports and class init)
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_guard_hierarchy_example1(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    hierarchy = gen._build_guard_hierarchy()

    # example1: all columns nullable
    assert hierarchy.mandatory_cols == []
    assert set(hierarchy.nullable_cols) == {
        "ssn", "empid", "name", "hdate", "phone", "email", "dept", "manager"
    }

    # 3 distinct guard levels: {}, {empid,hdate}, {empid,hdate,dept,manager}
    assert len(hierarchy.levels) == 3
    assert hierarchy.levels[0].guard_attrs == set()
    assert hierarchy.levels[1].guard_attrs == {"empid", "hdate"}
    assert hierarchy.levels[2].guard_attrs == {"empid", "hdate", "dept", "manager"}


def test_guard_hierarchy_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    hierarchy = gen._build_guard_hierarchy()

    # example2: ssn, name, phone, email are NOT nullable
    assert set(hierarchy.mandatory_cols) == {"ssn", "name", "phone", "email"}
    assert set(hierarchy.nullable_cols) == {"empid", "hdate", "dept", "manager"}

    # Same 3 levels
    assert len(hierarchy.levels) == 3

    # Level 0: all nullable cols are NULL
    assert hierarchy.levels[0].null_cols == ["empid", "hdate", "dept", "manager"]
    assert hierarchy.levels[0].not_null_cols == []

    # Level 1: empid, hdate NOT NULL; dept, manager NULL
    assert set(hierarchy.levels[1].not_null_cols) == {"empid", "hdate"}
    assert set(hierarchy.levels[1].null_cols) == {"dept", "manager"}

    # Level 2: all NOT NULL
    assert set(hierarchy.levels[2].not_null_cols) == {"empid", "hdate", "dept", "manager"}
    assert hierarchy.levels[2].null_cols == []
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest test/test_generator.py::test_guard_hierarchy_example1 -v`
Expected: FAIL — `Generator` has no `_build_guard_hierarchy` method

**Step 3: Implement GuardHierarchy**

Add to `src/sstc/generator.py` — new imports at top (line 10):
```python
from rapt2.treebrd.node import SelectNode, UnaryNode
```

Add dataclasses after `UnsupportedError` (after line 18):
```python
from dataclasses import dataclass, field


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
```

Add methods to `Generator` class (after `_extract_defined_attrs` at line 210):
```python
def _extract_table_guard_attrs(self, table: Table) -> list[str]:
    """Extract guard attributes from a target table's \select_{defined(...)} clause."""
    node = table.definition.child  # Skip AssignNode → ProjectNode
    while node is not None:
        if isinstance(node, SelectNode):
            return self._extract_defined_attrs(node.conditions)
        if isinstance(node, UnaryNode):
            node = node.child
        else:
            return []
    return []

def _build_guard_hierarchy(self) -> GuardHierarchy:
    """Build the specialization hierarchy from universal schema + target table guards."""
    schema = self.ctx.source.tables[0].universal_schema
    mandatory_cols = [a.name for a in schema if not a.is_nullable]
    nullable_cols = [a.name for a in schema if a.is_nullable]

    # Extract distinct guard sets from target tables
    guard_sets: dict[frozenset[str], list[str]] = {}
    for table in self.ctx.target.tables:
        guard = frozenset(self._extract_table_guard_attrs(table))
        guard_sets.setdefault(guard, []).append(table.name)

    # Always include empty guard (Level 0) even if no unguarded tables
    if frozenset() not in guard_sets:
        guard_sets[frozenset()] = []

    # Sort by cardinality ascending
    sorted_guards = sorted(guard_sets.items(), key=lambda x: len(x[0]))

    # Build levels with cumulative not_null / null columns
    levels = []
    for guard_frozen, tables in sorted_guards:
        guard_set = set(guard_frozen)
        # Cumulative: all guard attrs up to this level
        cumulative = set()
        for g, _ in sorted_guards:
            if len(g) <= len(guard_frozen):
                cumulative |= set(g)

        not_null = [c for c in nullable_cols if c in cumulative]
        null = [c for c in nullable_cols if c not in cumulative]

        levels.append(GuardLevel(
            guard_attrs=guard_set,
            tables=tables,
            not_null_cols=not_null,
            null_cols=null,
        ))

    src_pk: list[str] = []
    for t in self.ctx.source.tables:
        for col in self.ctx.source.primary_keys.get(t.name, []):
            if col not in src_pk:
                src_pk.append(col)

    return GuardHierarchy(
        mandatory_cols=mandatory_cols,
        nullable_cols=nullable_cols,
        levels=levels,
        source_pk=src_pk,
    )
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest test/test_generator.py::test_guard_hierarchy_example1 test/test_generator.py::test_guard_hierarchy_example2 -v`
Expected: PASS

**Step 5: Run full test suite to check regression**

Run: `uv run pytest test/ -v`
Expected: All existing tests PASS

**Step 6: Commit**

```bash
git add src/sstc/generator.py test/test_generator.py
git commit -m "feat: add GuardHierarchy abstraction and guard extraction from target tables"
```

---

### Task 3: CFD Exhaustive Checks (Gap 4)

**Files:**
- Create: `src/sstc/templates/cfd_check.sql.j2`
- Modify: `src/sstc/generator.py` (`_fd_sql` method at lines 212-250)
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_cfd_exhaustive_checks_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._constraints()

    # 3 CFD check functions (guarded FDs → CFD template)
    assert result.lower().count("check_person_source_cfd") >= 3

    # CFD_1 (empid → hdate, guard {empid, hdate}):
    # Main violation branch + 2 invalid states = 3 OR branches
    assert "R2.empid IS NULL AND R2.hdate IS NOT NULL" in result
    assert "R2.empid IS NOT NULL AND R2.hdate IS NULL" in result

    # CFD_2 (empid → dept, guard {empid, hdate, dept, manager}):
    # Main violation + invalid partial-null states
    assert "R2.empid IS NULL AND R2.dept IS NOT NULL" in result
    assert "R2.empid IS NULL AND R2.manager IS NOT NULL" in result
    assert "R2.dept IS NOT NULL AND R2.manager IS NULL" in result
    assert "R2.dept IS NULL AND R2.manager IS NOT NULL" in result

    # All use BEFORE INSERT triggers
    assert result.count("BEFORE INSERT") >= 3
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest test/test_generator.py::test_cfd_exhaustive_checks_example2 -v`
Expected: FAIL

**Step 3: Create the CFD template**

Write `src/sstc/templates/cfd_check.sql.j2`:
```
CREATE OR REPLACE FUNCTION {{ schema }}.check_{{ table_name }}_cfd_{{ fd_index }}_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF EXISTS (SELECT *
        FROM {{ schema }}._{{ table_name }} AS R1,
        (SELECT {{ new_cols }}) AS R2
        WHERE {{ where_branches | join('\n            OR ') }}) THEN
        RAISE EXCEPTION 'CFD violation on {{ table_name }}: {{ lhs_attrs | join(", ") }} -> {{ rhs_attrs | join(", ") }} %', NEW;
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER {{ table_name }}_cfd_{{ fd_index }}_trigger
BEFORE INSERT ON {{ schema }}._{{ table_name }}
FOR EACH ROW
EXECUTE FUNCTION {{ schema }}.check_{{ table_name }}_cfd_{{ fd_index }}_fn();
```

**Step 4: Add `_build_cfd_where_branches` and update `_fd_sql`**

Add to `Generator` class (after `_extract_table_guard_attrs`):
```python
def _build_cfd_where_branches(
    self,
    lhs_attrs: list[str],
    rhs_attrs: list[str],
    guard_attrs: list[str],
) -> list[str]:
    """Build exhaustive WHERE OR-branches for a CFD check."""
    branches: list[str] = []

    # Branch 1: Main FD violation — all guards non-NULL, LHS match, RHS differ
    guard_not_null = " AND ".join(f"R2.{a} IS NOT NULL" for a in guard_attrs)
    lhs_match = " AND ".join(f"R1.{a} = R2.{a}" for a in lhs_attrs)
    rhs_differ = " AND ".join(f"R1.{a} <> R2.{a}" for a in rhs_attrs)
    branches.append(f"({guard_not_null} AND {lhs_match} AND {rhs_differ})")

    # Invalid states: LHS NULL but RHS non-NULL
    for rhs_attr in rhs_attrs:
        for lhs_attr in lhs_attrs:
            branch = f"(R2.{lhs_attr} IS NULL AND R2.{rhs_attr} IS NOT NULL)"
            if branch not in branches:
                branches.append(branch)

    # Invalid states: non-LHS guard attr NULL but RHS non-NULL
    non_lhs_guards = [g for g in guard_attrs if g not in lhs_attrs]
    for rhs_attr in rhs_attrs:
        for g in non_lhs_guards:
            branch = f"(R2.{g} IS NULL AND R2.{rhs_attr} IS NOT NULL)"
            if branch not in branches:
                branches.append(branch)

    # Joint-null violations: paired guard attrs where one is NULL but other non-NULL
    rhs_and_paired = set(rhs_attrs) | set(non_lhs_guards)
    paired = [a for a in guard_attrs if a in rhs_and_paired]
    for i, a1 in enumerate(paired):
        for a2 in paired[i + 1 :]:
            for b in [
                f"(R2.{a1} IS NOT NULL AND R2.{a2} IS NULL)",
                f"(R2.{a1} IS NULL AND R2.{a2} IS NOT NULL)",
            ]:
                if b not in branches:
                    branches.append(b)

    return branches
```

Update `_fd_sql` method — replace lines 212-250 with:
```python
def _fd_sql(self, context: Context) -> str:
    fds = context.functional_dependencies
    if not fds:
        return ""

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
            # Guarded FD → CFD template with exhaustive OR branches
            where_branches = self._build_cfd_where_branches(
                lhs_attrs, rhs_attrs, guard_attrs
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
            # Unguarded FD → existing simple template
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
```

**Step 5: Update example1 tests for CFD naming**

In `test/test_generator.py`, update `test_fd_constraints`:
```python
def test_fd_constraints(example_1_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_1_dir, "universal.json"),
        source_path=os.path.join(example_1_dir, "source.txt"),
        target_path=os.path.join(example_1_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._constraints()

    # 3 CFD check functions for person_source (each has function + trigger = 6)
    assert result.lower().count("check_person_source_cfd") == 6

    # All have exhaustive OR branches with IS NOT NULL
    assert "IS NOT NULL" in result

    # Contains RAISE EXCEPTION and BEFORE INSERT trigger
    assert result.count("RAISE EXCEPTION") >= 3
    assert "BEFORE INSERT" in result
```

**Step 6: Run tests**

Run: `uv run pytest test/test_generator.py::test_cfd_exhaustive_checks_example2 test/test_generator.py::test_fd_constraints -v`
Expected: PASS

**Step 7: Commit**

```bash
git add src/sstc/generator.py src/sstc/templates/cfd_check.sql.j2 test/test_generator.py
git commit -m "feat: add exhaustive CFD checks with null-pattern enumeration (Gap 4)"
```

---

### Task 4: INC Trigger Enforcement (Gap 6)

**Files:**
- Create: `src/sstc/templates/inc_check.sql.j2`
- Modify: `src/sstc/generator.py` (add `_inc_sql`, update `_constraints`)
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_inc_constraint_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._constraints()

    # INC enforcement function exists
    assert "check_person_source_inc" in result.lower()

    # Allows NULL manager
    assert "IS NULL" in result

    # Uses EXCEPT pattern for existence check
    assert "EXCEPT" in result

    # BEFORE INSERT trigger
    assert "BEFORE INSERT" in result
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest test/test_generator.py::test_inc_constraint_example2 -v`
Expected: FAIL

**Step 3: Create INC template**

Write `src/sstc/templates/inc_check.sql.j2`:
```
CREATE OR REPLACE FUNCTION {{ schema }}.check_{{ table_name }}_inc_{{ inc_index }}_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    IF (NEW.{{ referencing_col }} IS NULL) THEN
        RETURN NEW;
    END IF;
    IF (NEW.{{ referencing_col }} = NEW.{{ self_ref_col }}) THEN
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT DISTINCT NEW.{{ referencing_col }}
        FROM {{ schema }}._{{ table_name }}
        EXCEPT (
            SELECT {{ referenced_col }} AS {{ referencing_col }}
            FROM {{ schema }}._{{ referenced_table }}
            UNION
            SELECT NEW.{{ self_ref_col }} AS {{ referencing_col }}
        )) THEN
        RAISE EXCEPTION 'INC violation: {{ table_name }}.{{ referencing_col }} ⊆ {{ referenced_table }}.{{ referenced_col }}';
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

CREATE TRIGGER {{ table_name }}_inc_{{ inc_index }}_trigger
BEFORE INSERT ON {{ schema }}._{{ table_name }}
FOR EACH ROW
EXECUTE FUNCTION {{ schema }}.check_{{ table_name }}_inc_{{ inc_index }}_fn();
```

**Step 4: Add `_inc_sql` method and integrate into `_constraints`**

Add to `Generator` class (after `_build_cfd_where_branches`):
```python
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
```

Update `_constraints` method (at line 252):
```python
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
```

**Step 5: Run tests**

Run: `uv run pytest test/test_generator.py::test_inc_constraint_example2 -v`
Expected: PASS

**Step 6: Update example1 full compile test for new counts**

INC adds 1 function + 1 trigger for example1's `inc⊆_{manager, empid} (Person_Source, Person_Source)`.

Update `test_full_compile_structure` in `test/test_generator.py`:
```python
# Functions: 2 MVD + 3 CFD + 1 INC + 18 capture + 18 join + 4 mapping = 46
fn_count = sql.count("CREATE OR REPLACE FUNCTION")
assert fn_count == 46, f"Expected 46 functions, got {fn_count}"

# Triggers: 2 MVD + 3 CFD + 1 INC + 18 capture + 18 join + 18 mapping = 60
trigger_count = sql.count("CREATE TRIGGER")
assert trigger_count == 60, f"Expected 60 triggers, got {trigger_count}"
```

**Step 7: Run full test suite**

Run: `uv run pytest test/ -v`
Expected: All PASS

**Step 8: Commit**

```bash
git add src/sstc/generator.py src/sstc/templates/inc_check.sql.j2 test/test_generator.py
git commit -m "feat: add trigger-based INC enforcement for intra-table inclusion deps (Gap 6)"
```

---

### Task 5: Conditional INSERTs in SOURCE_INSERT_FN (Gap 1)

**Files:**
- Modify: `src/sstc/generator.py` (`_mapping` method at lines 357-464)
- Modify: `src/sstc/templates/insert_mapping.sql.j2`
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_conditional_inserts_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # SOURCE_INSERT_FN should have IF EXISTS guards for guarded tables
    # Tables PE, PE_HDATE have guard {empid, hdate}
    # Tables PED, PEDDept, DeptManager have guard {empid, hdate, dept, manager}
    # Tables P, PersonPhone, PersonEmail have NO guard (unconditional INSERT)

    # Extract just the SOURCE_INSERT_FN section
    source_fn_start = result.index("SOURCE_INSERT_FN")
    source_fn_end = result.index("TARGET_INSERT_FN")
    source_fn = result[source_fn_start:source_fn_end]

    # Guarded tables should have IF EXISTS wrapping
    assert "IF EXISTS" in source_fn
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in source_fn

    # Unguarded tables (P, PersonPhone, PersonEmail) should NOT be wrapped
    # They appear as direct INSERT without IF EXISTS preceding them
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest test/test_generator.py::test_conditional_inserts_example2 -v`
Expected: FAIL

**Step 3: Update generator to pass guard info**

In `_mapping()` method, update the `tgt_tables_info` construction (around line 375):
```python
tgt_tables_info = [
    {
        "name": t.name,
        "attrs": t.attributes,
        "pk": target.primary_keys.get(t.name, []),
        "guard_check": " AND ".join(
            f"{a} IS NOT NULL"
            for a in self._extract_table_guard_attrs(t)
        ),
    }
    for t in target.tables
]
```

**Step 4: Update insert_mapping.sql.j2 template**

Replace the `{% else %}` block (the `use_temp_join=False` / SOURCE path, lines 36-44) with:
```jinja2
{% else %}
{% for tbl in target_tables %}
{% if tbl.guard_check %}
    IF EXISTS (SELECT * FROM {{ schema }}._{{ source_tables[0] }}_{{ suffix }}_JOIN
              WHERE {{ tbl.guard_check }}) THEN
{% endif %}
    INSERT INTO {{ schema }}._{{ tbl.name }} (SELECT DISTINCT {{ tbl.attrs | join(', ') }}
        FROM {{ schema }}._{{ source_tables[0] }}_{{ suffix }}_JOIN
{% for src in source_tables[1:] %}
        NATURAL LEFT OUTER JOIN {{ schema }}._{{ src }}_{{ suffix }}_JOIN
{% endfor %}
        WHERE {{ tbl.where_not_null }})
        ON CONFLICT{% if tbl.pk %} ({{ tbl.pk | join(', ') }}){% endif %} DO NOTHING;
{% if tbl.guard_check %}
    END IF;
{% endif %}
{% endfor %}
{% endif %}
```

**Step 5: Run tests**

Run: `uv run pytest test/test_generator.py::test_conditional_inserts_example2 test/test_generator.py::test_source_insert_mapping -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/sstc/generator.py src/sstc/templates/insert_mapping.sql.j2 test/test_generator.py
git commit -m "feat: add conditional IF EXISTS guards for source→target INSERTs (Gap 1)"
```

---

### Task 6: Null-Pattern WHERE in TARGET_INSERT_FN (Gap 2)

**Files:**
- Modify: `src/sstc/generator.py` (`_mapping` method)
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_null_pattern_where_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Extract TARGET_INSERT_FN section
    target_fn_start = result.index("TARGET_INSERT_FN")
    target_fn_end = result.index("SOURCE_DELETE_FN")
    target_fn = result[target_fn_start:target_fn_end]

    # Mandatory cols always NOT NULL
    assert "ssn IS NOT NULL" in target_fn
    assert "name IS NOT NULL" in target_fn
    assert "phone IS NOT NULL" in target_fn
    assert "email IS NOT NULL" in target_fn

    # Null-pattern disjunction (not all-NOT-NULL)
    assert "empid IS NULL AND hdate IS NULL" in target_fn
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in target_fn

    # Should NOT have "empid IS NOT NULL" as a standalone requirement
    # (it's conditional, not mandatory)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest test/test_generator.py::test_null_pattern_where_example2 -v`
Expected: FAIL

**Step 3: Add `_build_null_pattern_where` and update `_mapping`**

Add to `Generator` class:
```python
def _build_null_pattern_where(self, hierarchy: GuardHierarchy) -> str:
    """Build WHERE clause with valid null-pattern disjunction."""
    parts = []

    # Mandatory columns always NOT NULL
    if hierarchy.mandatory_cols:
        parts.append(
            " AND ".join(f"{c} IS NOT NULL" for c in hierarchy.mandatory_cols)
        )

    if not hierarchy.nullable_cols:
        return " AND ".join(parts) if parts else "TRUE"

    # Valid null-pattern branches (one per hierarchy level)
    branches = []
    for level in hierarchy.levels:
        branch_parts = []
        for col in hierarchy.nullable_cols:
            if col in level.not_null_cols:
                branch_parts.append(f"{col} IS NOT NULL")
            else:
                branch_parts.append(f"{col} IS NULL")
        branches.append("(" + " AND ".join(branch_parts) + ")")

    pattern_clause = "(" + " OR ".join(branches) + ")"
    parts.append(pattern_clause)

    return " AND ".join(parts)
```

In `_mapping()`, replace line 412:
```python
# Old:
# tgt_insert_where = " AND ".join(f"{a} IS NOT NULL" for a in universal_col_names)
# New:
hierarchy = self._build_guard_hierarchy()
tgt_insert_where = self._build_null_pattern_where(hierarchy)
```

**Step 4: Run tests**

Run: `uv run pytest test/test_generator.py::test_null_pattern_where_example2 test/test_generator.py::test_target_insert_mapping -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sstc/generator.py test/test_generator.py
git commit -m "feat: generate null-pattern WHERE disjunction for target→source join (Gap 2)"
```

---

### Task 7: Tuple Containment Pruning (Gap 3)

**Files:**
- Modify: `src/sstc/generator.py` (`_mapping` method)
- Modify: `src/sstc/templates/insert_mapping.sql.j2`
- Test: `test/test_generator.py`

**Step 1: Write the failing test**

Add to `test/test_generator.py`:
```python
def test_tuple_containment_pruning_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    gen = Generator(ctx)
    result = gen._mapping()

    # Extract TARGET_INSERT_FN section
    target_fn_start = result.index("TARGET_INSERT_FN")
    target_fn_end = result.index("SOURCE_DELETE_FN")
    target_fn = result[target_fn_start:target_fn_end]

    # Tuple containment pruning should appear AFTER temp_table_join INSERT
    # and BEFORE INSERT INTO source
    assert "DELETE FROM temp_table_join" in target_fn

    # Should check for richer tuples at Level 1 (empid, hdate non-null)
    assert "empid IS NOT NULL AND hdate IS NOT NULL" in target_fn

    # Should delete poorer tuples where nullable cols are NULL
    assert "empid IS NULL" in target_fn
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest test/test_generator.py::test_tuple_containment_pruning_example2 -v`
Expected: FAIL

**Step 3: Add `_build_containment_pruning` method**

Add to `Generator` class:
```python
def _build_containment_pruning(
    self, hierarchy: GuardHierarchy
) -> list[dict]:
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

        richer_condition = " AND ".join(
            f"{c} IS NOT NULL" for c in richer.not_null_cols
        )
        poorer_condition = " AND ".join(
            f"{c} IS NULL" for c in new_not_null
        )
        identity_match = " AND ".join(
            f"t_rich.{c} = t_poor.{c}"
            for c in (hierarchy.mandatory_cols or hierarchy.source_pk)
        )

        rules.append({
            "richer_condition": richer_condition,
            "poorer_condition": poorer_condition,
            "identity_match": identity_match,
        })

    return rules
```

**Step 4: Pass pruning rules to template**

In `_mapping()`, before the TARGET_INSERT_FN render call, compute and pass `prune_rules`:
```python
prune_rules = self._build_containment_pruning(hierarchy)
```

Add `prune_rules=prune_rules` to the TARGET_INSERT_FN `_render` call.

**Step 5: Update insert_mapping.sql.j2**

Add after the `INSERT INTO temp_table_join` block and BEFORE the `{% for tbl in target_tables %}` loop (inside the `{% if use_temp_join %}` block):
```jinja2
{% if prune_rules is defined and prune_rules %}
    -- Tuple containment: keep only most informative tuples
{% for rule in prune_rules %}
    IF EXISTS (SELECT * FROM temp_table_join WHERE {{ rule.richer_condition }}) THEN
        DELETE FROM temp_table_join t_poor
        WHERE {{ rule.poorer_condition }}
        AND EXISTS (
            SELECT 1 FROM temp_table_join t_rich
            WHERE {{ rule.identity_match }}
            AND {{ rule.richer_condition }}
        );
    END IF;
{% endfor %}
{% endif %}
```

**Step 6: Run tests**

Run: `uv run pytest test/test_generator.py::test_tuple_containment_pruning_example2 test/test_generator.py::test_target_insert_mapping -v`
Expected: PASS

**Step 7: Commit**

```bash
git add src/sstc/generator.py src/sstc/templates/insert_mapping.sql.j2 test/test_generator.py
git commit -m "feat: add tuple containment pruning for nullable T→S reconstruction (Gap 3)"
```

---

### Task 8: Full Compile Validation for Example 2

**Files:**
- Test: `test/test_generator.py`

**Step 1: Write the full compile test for example2**

Add to `test/test_generator.py`:
```python
def test_full_compile_example2(example_2_dir: str):
    ctx = TransducerContext.from_files(
        universal_path=os.path.join(example_2_dir, "universal.json"),
        source_path=os.path.join(example_2_dir, "source.txt"),
        target_path=os.path.join(example_2_dir, "target.txt"),
    )
    sql = Generator(ctx).compile()

    # Schema infrastructure
    assert "DROP SCHEMA IF EXISTS transducer CASCADE" in sql
    assert "CREATE SCHEMA transducer" in sql
    assert "CREATE TABLE transducer._loop" in sql

    # Base tables: 1 source + 8 target = 9
    # Tracking: 9 × 2 = 18
    # Join staging: 9 × 2 = 18
    # Total CREATE TABLE: 9 + 18 + 18 + 1 (loop) = 46
    create_count = sql.count("CREATE TABLE transducer.")
    assert create_count == 46, f"Expected 46 CREATE TABLE, got {create_count}"

    # Functions: 2 MVD + 3 CFD + 1 INC + 18 capture + 18 join + 4 mapping = 46
    fn_count = sql.count("CREATE OR REPLACE FUNCTION")
    assert fn_count == 46, f"Expected 46 functions, got {fn_count}"

    # Triggers: 2 MVD + 3 CFD + 1 INC + 18 capture + 18 join + 18 mapping = 60
    trigger_count = sql.count("CREATE TRIGGER")
    assert trigger_count == 60, f"Expected 60 triggers, got {trigger_count}"

    # Composite PK on source
    assert "PRIMARY KEY (ssn, phone, email)" in sql

    # NOT NULL on mandatory columns
    assert "ssn VARCHAR(100) NOT NULL" in sql
    assert "name VARCHAR(100) NOT NULL" in sql

    # Nullable columns without NOT NULL
    # (empid should NOT have NOT NULL in source table)
    assert "empid VARCHAR(100)," in sql or "empid VARCHAR(100)\n" in sql

    # Mapping functions present
    assert "SOURCE_INSERT_FN" in sql
    assert "TARGET_INSERT_FN" in sql
    assert "SOURCE_DELETE_FN" in sql
    assert "TARGET_DELETE_FN" in sql

    # Key patterns from design
    assert "IF EXISTS" in sql  # Conditional INSERTs
    assert "empid IS NULL AND hdate IS NULL" in sql  # Null-pattern WHERE
    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql
    assert "NATURAL LEFT OUTER JOIN" in sql
    assert "ABS(loop_start)" in sql
```

**Step 2: Run full test suite**

Run: `uv run pytest test/ -v`
Expected: All PASS

**Step 3: Commit**

```bash
git add test/test_generator.py
git commit -m "test: add full compile validation for example2 with all gap patches"
```

---

### Task 9: Final Regression Check and Lint

**Step 1: Run all tests**

Run: `uv run pytest test/ -v`
Expected: All PASS

**Step 2: Run linter**

Run: `uv run ruff check .`
Expected: No errors

**Step 3: Run formatter**

Run: `uv run ruff format .`

**Step 4: Final commit if formatting changed anything**

```bash
git add -u
git commit -m "style: apply ruff formatting"
```
