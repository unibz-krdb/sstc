# Guard Hierarchy Abstraction — Design Document

**Date:** 2026-04-02
**Approach:** B — Guard Hierarchy Abstraction
**Status:** Approved

## Problem

The current SSTC compiler was developed against Example 1 (all columns nullable, single-column PK, simple FD guards). The new reference example (`docs/notes/example/`) introduces mixed nullability in the Universal Table, a composite PK, and conditional functional dependencies (CFDs) with complex guard interactions. Seven gaps were identified between the current system and the new example.

## Core Insight

The four critical gaps (conditional INSERTs, null-pattern WHERE, tuple containment, CFD checks) all derive from one concept: target tables form a **specialization hierarchy** defined by their `\select_{defined(...)}` guards. Computing this hierarchy once and sharing it across all downstream SQL generation is both cleaner and more correct than reimplementing the analysis in each place.

## Core Abstraction: GuardHierarchy

A new data structure computed once from the universal schema and target table guards:

```python
@dataclass
class GuardLevel:
    guard_attrs: set[str]      # e.g. {"empid", "hdate"}
    tables: list[str]          # target tables at this level
    not_null_cols: list[str]   # cols that must be NOT NULL at this level
    null_cols: list[str]       # cols that must be NULL at this level

@dataclass
class GuardHierarchy:
    mandatory_cols: list[str]  # always NOT NULL (is_nullable=false)
    nullable_cols: list[str]   # can be NULL (is_nullable=true)
    levels: list[GuardLevel]   # sorted by guard_attrs cardinality ascending
    source_pk: list[str]       # source table's PK columns
```

### How It's Built

1. Read `is_nullable` from universal schema → split into `mandatory_cols` / `nullable_cols`
2. For each target table, extract guard attrs by traversing `AssignNode → ProjectNode → SelectNode` and calling `_extract_defined_attrs()`
3. Collect distinct guard-attr sets, sort by cardinality ascending
4. For each level, compute `not_null_cols` (cumulative union of guards) and `null_cols` (remaining nullable cols)

### Guard Extraction

Traverse the RAPT2 AST — walk the `child` chain of `UnaryNode` subtypes until a `SelectNode` is found, then extract its `defined()` conditions. This reuses the existing `_extract_defined_attrs()` method.

### Example Output (PERSON)

```
mandatory_cols: [ssn, name, phone, email]
nullable_cols:  [empid, hdate, dept, manager]
levels:
  Level 0: guard={},                          not_null=[], null=[empid,hdate,dept,manager]
  Level 1: guard={empid,hdate},               not_null=[empid,hdate], null=[dept,manager]
  Level 2: guard={empid,hdate,dept,manager},  not_null=[empid,hdate,dept,manager], null=[]
```

---

## Gap 1: Conditional INSERTs in SOURCE_INSERT_FN

**Problem:** All target table INSERTs are unconditional. Subtables should only receive rows when their defining attributes are non-NULL.

**Solution:** Each target table carries its guard attrs. Tables with non-empty guards get wrapped in `IF EXISTS` checks. The guard check is the conjunction of `attr IS NOT NULL` for each guard attribute.

**Changes:**
- `generator.py`: Pass `guard_check` string per target table in `_mapping()`
- `insert_mapping.sql.j2`: Wrap each INSERT in `IF EXISTS ... END IF` when `guard_check` is non-empty

---

## Gap 2: NULL-Pattern WHERE in TARGET_INSERT_FN

**Problem:** The `tgt_insert_where` currently requires ALL columns NOT NULL. With nullable columns, a disjunction of valid null-patterns is needed.

**Solution:** Use `GuardHierarchy.levels` to generate a WHERE clause with mandatory NOT NULL columns ANDed with an OR-disjunction of valid null patterns (one branch per level).

**Generated output:**
```sql
WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
  AND ((empid IS NULL AND hdate IS NULL AND dept IS NULL AND manager IS NULL)
    OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL AND manager IS NULL)
    OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL))
```

**Changes:**
- `generator.py`: New method `_build_null_pattern_where()` replaces the simple `tgt_insert_where` computation

---

## Gap 3: Tuple Containment Pruning

**Problem:** After LEFT OUTER JOIN reconstruction, redundant "poorer" tuples (with more NULLs) appear alongside "richer" ones.

**Solution:** Generate IF/DELETE blocks that iterate hierarchy levels from richest to poorest, deleting poorer tuples when a richer version with the same identity exists.

**Generated output:**
```sql
IF EXISTS (SELECT * FROM temp_table_join WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
    DELETE FROM temp_table_join t_poor
    WHERE empid IS NULL AND hdate IS NULL
    AND EXISTS (
        SELECT 1 FROM temp_table_join t_rich
        WHERE t_rich.ssn = t_poor.ssn AND t_rich.phone = t_poor.phone AND t_rich.email = t_poor.email
        AND empid IS NOT NULL AND hdate IS NOT NULL
    );
END IF;
```

**Changes:**
- `generator.py`: New method `_build_containment_pruning()` produces pruning rules from the hierarchy
- `insert_mapping.sql.j2`: New section after temp_table_join INSERT, before INSERT into source

---

## Gap 4: CFD Exhaustive Checks

**Problem:** Current `fd_check.sql.j2` uses a simple `IF guard THEN check` pattern. CFDs need exhaustive OR-branch enumeration of all invalid null-pattern states.

**Solution:** For guarded FDs, generate a single `IF EXISTS` with OR branches covering:
1. Main FD violation (all guard attrs non-NULL, LHS match, RHS differ)
2. Invalid states: RHS non-NULL but LHS NULL
3. Invalid states: non-LHS guard attr NULL but RHS non-NULL
4. Invalid states: jointly-null guard attr pairs where one is NULL and the other non-NULL

**Changes:**
- `generator.py`: New method `_build_cfd_where_branches()`, update `_fd_sql()` to route guarded FDs to new template
- New template `cfd_check.sql.j2` for exhaustive checks
- Existing `fd_check.sql.j2` retained for unguarded FDs

---

## Gap 5: Composite PK

**Status: Already works.** Templates use `{{ pk_columns | join(', ') }}` which handles arbitrary cardinality. Only new test coverage needed.

---

## Gap 6: INC Trigger Enforcement

**Problem:** Intra-table inclusion dependencies (e.g., `manager ⊆ ssn` on same table) need trigger-based enforcement, not just FK constraints.

**Solution:** Detect intra-table INC (both relation names identical) and generate a BEFORE INSERT trigger that:
1. Allows NULL values
2. Allows self-reference (value equals PK)
3. Checks existence via SELECT/EXCEPT pattern

**Changes:**
- New template `inc_check.sql.j2`
- `generator.py`: New method `_inc_sql()`, integrate into `_constraints()`

---

## Gap 7: FK Direction

**Status: Already works.** The `inc=_{dept, dept} (PEDDept, DeptManager)` declaration correctly generates `_DEPT_MANAGER FK(dept) → _PED_DEPT(dept)` via existing equivalence logic. Input controls direction.

---

## New Test Inputs (example2)

A new `test/inputs/example2/` directory with:
- `universal.json`: Mixed nullability (ssn, name, phone, email NOT NULL; empid, hdate, dept, manager nullable)
- `source.txt`: Composite PK `(ssn, phone, email)`, INC `manager ⊆ ssn`
- `target.txt`: 8 tables with guards, reversed FK via `inc=_{dept, dept} (PEDDept, DeptManager)`

---

## Implementation Order

| Phase | Tasks | Dependencies |
|-------|-------|--------------|
| 1. Foundation | `GuardHierarchy` dataclass + `_extract_table_guard_attrs()` + `_build_guard_hierarchy()` | None |
| 2. CFD Checks | `cfd_check.sql.j2` + `_build_cfd_where_branches()` + update `_fd_sql()` | Phase 1 |
| 3. Conditional INSERTs | Update `insert_mapping.sql.j2` SOURCE path + pass guard info in `_mapping()` | Phase 1 |
| 4. Null-Pattern WHERE | `_build_null_pattern_where()` + update TARGET path in `_mapping()` | Phase 1 |
| 5. Tuple Containment | `_build_containment_pruning()` + new template section | Phase 1, 4 |
| 6. INC Triggers | `inc_check.sql.j2` + `_inc_sql()` + integrate into `_constraints()` | None |
| 7. Test Inputs + Validation | New `example2/` fixtures + test cases | Phases 1–6 |

## File Change Summary

| File | Change Type | What |
|------|-------------|------|
| `src/sstc/generator.py` | Modify | Add GuardHierarchy, extraction, hierarchy building, null-pattern WHERE, containment pruning, CFD branches, INC generation. Update `_fd_sql()`, `_mapping()`, `_constraints()` |
| `src/sstc/templates/insert_mapping.sql.j2` | Modify | Add IF EXISTS guards, null-pattern WHERE, tuple containment pruning section |
| `src/sstc/templates/cfd_check.sql.j2` | Create | Exhaustive CFD checks with OR branches |
| `src/sstc/templates/inc_check.sql.j2` | Create | INC trigger enforcement |
| `test/inputs/example2/` | Create | `universal.json`, `source.txt`, `target.txt` |
| `test/test_generator.py` | Modify | Add example2 tests validating all gaps |
