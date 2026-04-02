# Nullable Guard Hierarchy Design

**Date:** 2026-04-02
**Status:** Approved
**Problem:** The current SSTC generator assumes all universal columns are nullable but then requires all to be NOT NULL in mapping functions — never exercising true nullable code paths. The new reference example (`docs/notes/example/`) has mixed nullability, which breaks 7 areas of SQL generation.

## Context

The new PERSON URA example differs from the old test inputs in:
- **Mixed nullability**: ssn, name, phone, email are NOT NULL; empid, hdate, dept, manager are nullable
- **Composite source PK**: `(ssn, phone, email)` vs single `(ssn)`
- **INC constraint**: `manager ⊆ ssn` (not empid), needs trigger-based enforcement
- **CFD guards**: require exhaustive invalid-state enumeration, not simple IS NOT NULL checks
- **Conditional INSERTs**: target tables only receive rows when defining attrs are non-NULL
- **NULL-pattern WHERE**: T→S join must enumerate valid null combinations, not require all NOT NULL
- **Tuple containment**: redundant tuples from LEFT OUTER JOIN must be pruned by information content

## Approach: Guard Hierarchy Abstraction

Target table `\select_{defined(...)}` guards form an ordered specialization hierarchy:

```
Level 0: {}                              → _P, _PERSON_PHONE, _PERSON_EMAIL
Level 1: {empid, hdate}                  → _PE, _PE_HDATE
Level 2: {empid, hdate, dept, manager}   → _PED, _PED_DEPT, _DEPT_MANAGER
```

A new `GuardHierarchy` class extracts this from the parsed AST and provides methods that drive all nullable-aware SQL generation.

## Data Model

```python
@dataclass
class GuardLevel:
    guard_attrs: frozenset[str]   # Attrs that must be NOT NULL at this level
    tables: list[str]             # Target tables at this level

@dataclass
class GuardHierarchy:
    levels: list[GuardLevel]      # Sorted by guard size ascending
    non_nullable_cols: list[str]  # Universal cols where is_nullable=False
    nullable_cols: list[str]      # Universal cols where is_nullable=True
```

## Methods

### `conditional_insert_guard(table_name) -> list[str] | None`
**Solves Gap 1.** Returns guard attrs for a target table's conditional INSERT, or None if unconditional (Level 0). Used to wrap target INSERTs in `IF EXISTS (SELECT * FROM ... WHERE guard IS NOT NULL)`.

### `valid_null_patterns() -> list[dict]`
**Solves Gap 2.** Computes the WHERE clause disjunction for the T→S join temp table.

Algorithm:
1. Sort levels by guard size ascending: L₀, L₁, ..., Lₙ
2. Compute deltas: Δᵢ = Lᵢ.guard_attrs \ Lᵢ₋₁.guard_attrs
3. Generate branches:
   - Branch 0 (base): all attrs in Δ₁ are NULL
   - Branch i (1 ≤ i < n): all attrs in Lᵢ NOT NULL, all attrs in Δᵢ₊₁ are NULL
   - Branch n (max): all attrs in Lₙ NOT NULL

Generates the full version (checking all delta attrs) rather than the shortened version from the reference notes, as the author recommends for code generation.

### `containment_pruning_checks() -> list[dict]`
**Solves Gap 3.** Returns nested IF/EXCEPT/DELETE checks for tuple containment pruning.

Traverses the hierarchy top-down: for n levels, generates n-1 nested checks. Each check uses a discriminant attribute from the level boundary delta to identify and remove less-informative tuples.

### `cfd_invalid_patterns(guard_attrs, hierarchy) -> list[dict]`
**Solves Gap 4.** Enumerates all invalid partial-NULL states for a CFD's guard attributes, using the hierarchy to determine which combinations are valid vs invalid. The main FD violation check is combined with invalid-state OR branches in the WHERE clause.

## Template Changes

- `insert_mapping.sql.j2`: Add IF EXISTS guards for conditional INSERTs, replace all-NOT-NULL WHERE with null-pattern disjunction, add tuple containment pruning block
- `fd_check.sql.j2`: Rewrite to generate multi-branch OR WHERE clause for CFDs (keep simple behavior for non-guarded FDs)
- `inc_check.sql.j2` (new): NULL-aware trigger enforcement for intra-table inclusion dependencies

## Implementation Phases

### Phase 1: Foundation — GuardHierarchy + Test Inputs
New `src/sstc/guards.py`, `test/inputs/example2/*`, `test/test_guards.py`. Pure computation, no template changes, existing tests stay green.

### Phase 2: Conditional INSERTs (Gap 1) + NULL-Pattern WHERE (Gap 2)
Modify `generator.py` and `insert_mapping.sql.j2`. Wire GuardHierarchy into `_mapping()`.

### Phase 3: Tuple Containment (Gap 3)
Add nested IF/EXCEPT/DELETE block to `insert_mapping.sql.j2`.

### Phase 4: CFD Exhaustive Checks (Gap 4)
Rewrite `fd_check.sql.j2` and update `_fd_sql()` in generator.

### Phase 5: INC Triggers + Composite PK Verification (Gaps 5-6)
New `inc_check.sql.j2`, intra-table INC detection in `_constraints()`.

### Phase 6: Integration Validation
Example2 test suite, validate against reference SQL, ensure example1 still passes.

## Dependencies

```
Phase 1 → Phase 2 → Phase 3
Phase 1 → Phase 4
Phase 5 (independent)
Phases 2-5 → Phase 6
```

## What Stays Unchanged

- context.py, table.py, definition.py, transducer.py, transducer_context.py
- All templates except insert_mapping.sql.j2 and fd_check.sql.j2
- All example1 tests
