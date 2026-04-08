# Pipeline Walkthrough: PERSON URA Example

How the SSTC compiler processes `test/inputs/example1/` — from relational algebra input to ~1,950 lines of PostgreSQL. Each section corresponds to a stage in the `Generator.compile()` pipeline.

---

## Input Files

Three files define the transduction:

**`universal.json`** — 8 attributes, all `VARCHAR(100)`, all nullable:

```
ssn, empid, name, hdate, phone, email, dept, manager
```

All attributes are nullable, which is critical: NULLs encode which specialization level a tuple belongs to. A person without an employer has `empid`, `hdate`, `dept`, `manager` all NULL.

**`source.txt`** — A single wide table (`Person_Source`) with all 8 columns, plus constraints:

```
Person_Source := \project_{ssn, empid, name, hdate, phone, email, dept, manager} Universal;
pk_{ssn} Person_Source;
mvd_{ssn, phone} Person_Source;
mvd_{ssn, email} Person_Source;
fd_{empid, hdate} \select_{defined(empid) and defined(hdate)} Person_Source;
fd_{empid, dept} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Person_Source;
fd_{dept, manager} \select_{defined(empid) and defined(hdate) and defined(dept) and defined(manager)} Person_Source;
inc⊆_{manager, empid} (Person_Source, Person_Source);

UniversalMapping := \project_{ssn, empid, name, hdate, phone, email, dept, manager} Person_Source;
```

This declares: one primary key, two multivalued dependencies (phone and email are independent of each other for a given ssn), three conditional functional dependencies (guarded by `defined(...)` clauses), and one intra-table inclusion dependency (every manager must be an employee).

**`target.txt`** — 8 normalized tables decomposing the source:

| Table | Attributes | PK |
|---|---|---|
| Person | ssn, name | ssn |
| PersonPhone | ssn, phone | ssn, phone |
| PersonEmail | ssn, email | ssn, email |
| Employee | ssn, empid | empid |
| EmployeeDate | empid, hdate | empid |
| PED | ssn, empid | empid |
| PEDDept | empid, dept | empid |
| DeptManager | dept, manager | dept |

Plus 5 equality inclusion dependencies (`inc=`), 3 subsumption inclusion dependencies (`inc⊆`), and a `UniversalMapping` that reconstructs the universal tuple via `NATURAL JOIN` of all 8 tables.

---

## Stage 1: Parsing via RAPT2

**Entry point:** `TransducerContext.from_files()` calls `Context.from_file()` twice — once per direction.

**Phase A — Schema bootstrap** (`context.py:104-110`): The JSON is loaded into a list of `AttributeSchema` objects and a flat dict `{"Universal": ["ssn", "empid", ...]}` that RAPT2 requires for column name resolution.

**Phase B — RAPT2 parse** (`context.py:119-131`): The relational algebra text is fed to `Rapt(grammar="Dependency Grammar").to_syntax_tree()`, which produces a flat stream of nodes. The loop classifies each node:

- `AssignNode` named `universalmapping` (case-insensitive check) is stored as `universal_mapping`
- Other `AssignNode`s are collected into a `relations` list
- `DependencyNode`s (PK, FD, MVD, INC variants) are collected into a `dependencies` list
- Anything else raises `ValueError`

### Source parse results

| Node type | Count | Instances |
|---|---|---|
| `AssignNode` | 1 | `person_source` (RAPT2 lowercases names) |
| `PrimaryKeyNode` | 1 | `pk_{ssn}` |
| `MultivaluedDependencyNode` | 2 | `mvd_{ssn, phone}`, `mvd_{ssn, email}` |
| `FunctionalDependencyNode` | 3 | `fd_{empid, hdate}`, `fd_{empid, dept}`, `fd_{dept, manager}` — each wrapping a `SelectNode` |
| `InclusionSubsumptionNode` | 1 | `inc⊆_{manager, empid}` (intra-table) |
| `AssignNode` (mapping) | 1 | `UniversalMapping` |

### Target parse results

| Node type | Count |
|---|---|
| `AssignNode` | 8 (one per table) |
| `PrimaryKeyNode` | 8 |
| `InclusionEquivalenceNode` | 5 (`inc=`) |
| `InclusionSubsumptionNode` | 3 (`inc⊆`) |
| `AssignNode` (mapping) | 1 |

---

## Stage 2: Table Construction

`Table.from_relations_and_dependencies()` (`table.py:38-64`) matches each `AssignNode` to its dependency nodes:

- **Unary** dependencies (PK, FD, MVD) match by `relation_name == definition.name`
- **Binary** dependencies (INC) match if **either** child's name matches — so binary constraints are attached to both participating tables

**Source context:** 1 `Table` object (`person_source`) with all 7 dependency nodes.

**Target context:** 8 `Table` objects. Each carries its PK node, plus any INC nodes where it participates. For example, `Employee` carries both `inc=_{empid, empid}(Employee, EmployeeDate)` and `inc⊆_{ssn, ssn}(Employee, Person)`.

---

## Stage 3: Context Property Resolution

`Context.__init__` builds a `Schema` mapping `{table_name: [attrs]}`. Cached properties lazily filter dependency nodes by type:

| Property | Source value | Target value |
|---|---|---|
| `primary_keys` | `{"person_source": ["ssn"]}` | 8 entries, one per table |
| `functional_dependencies` | 3 FD nodes | (empty) |
| `multivalued_dependencies` | 2 MVD nodes | (empty) |
| `inclusion_equivalences` | (empty) | 5 nodes |
| `inclusion_subsumptions` | 1 node | 3 nodes |

All constraints live on the source side except the inter-table INCs, which are on the target side. This reflects the example's structure: the source is a wide denormalized table with complex intra-table constraints, while the target tables are simple (PK-only) and related through inter-table inclusion dependencies.

---

## Stage 4: Generator Compilation

`Generator(ctx).compile()` (`generator.py:67-88`) validates exactly 1 source table, then assembles 7 sections. Each section calls `self._render()` to populate Jinja2 templates from `src/sstc/templates/`.

### 4.1 Preamble

Template: `preamble.sql.j2`

```sql
DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;
CREATE TABLE transducer._loop (loop_start INT NOT NULL);
```

The `_loop` table is the cycle-detection mechanism. It prevents infinite cascading when bidirectional triggers fire.

### 4.2 Base Tables

Template: `create_table.sql.j2` (rendered 9 times)

For each table in both contexts, `_table_columns()` maps attribute names back to the universal schema to resolve data types and nullability. Produces 9 `CREATE TABLE` statements with `PRIMARY KEY` clauses.

In this example, all columns are nullable (no `NOT NULL` constraints), because all universal attributes have `is_nullable: true`. Only the primary key constraint enforces non-nullability implicitly.

### 4.3 Foreign Keys

Function: `foreign_keys()` in `constraints.py`

Iterates both contexts' inclusion dependencies and emits `ALTER TABLE ADD FOREIGN KEY` when the referenced columns form the referenced table's primary key.

**Direction rules:**
- **Equivalence INCs** (`inc=`): direction is swapped — the second relation references the first. `inc=_{ssn, ssn}(Person, PersonPhone)` becomes `PersonPhone REFERENCES Person(ssn)`.
- **Subsumption INCs** (`inc⊆`): first references second. `inc⊆_{ssn, ssn}(Employee, Person)` becomes `Employee(ssn) REFERENCES Person(ssn)`.

The source-side `inc⊆_{manager, empid}` does **not** produce an FK — it's an intra-table constraint handled by a trigger instead (see 4.4c).

**Output:** 7 `ALTER TABLE ADD FOREIGN KEY` statements:

| FK | Referencing | Referenced |
|---|---|---|
| 1 | PersonPhone(ssn) | Person(ssn) |
| 2 | PersonEmail(ssn) | Person(ssn) |
| 3 | EmployeeDate(empid) | Employee(empid) |
| 4 | PEDDept(empid) | PED(empid) |
| 5 | Employee(ssn) | Person(ssn) |
| 6 | PED(empid) | Employee(empid) |
| 7 | DeptManager(manager) | Employee(empid) |

### 4.4 Constraints

Function: `constraints()` in `constraints.py`

Generates enforcement triggers for both contexts. For this example, all constraints are on the source side.

#### 4.4a MVD Enforcement

Templates: `mvd_check.sql.j2`, `mvd_grounding.sql.j2`

Two MVDs share LHS `ssn`: `mvd_{ssn, phone}` and `mvd_{ssn, email}`.

**MVD Check (BEFORE INSERT):** Cross-products existing tuples (`r1`) with the NEW row (`r2`) where `r1.ssn = r2.ssn`, selecting `r1` values for LHS+determined attributes and `r2` values for the rest. If `EXCEPT` against existing tuples is non-empty, the MVD is violated.

Concretely, the SELECT is:
```sql
SELECT DISTINCT r1.ssn, r2.empid, r2.name, r2.hdate, r1.phone, r1.email, r2.dept, r2.manager
```

The `r1.phone, r1.email` come from existing tuples; everything else from NEW. If this produces rows not already in the table, the insert would violate the independence of phone/email from other attributes.

**MVD Grounding (AFTER INSERT):** Inserts complementary tuples to restore 4NF. For each determined attribute (phone, email), generates a `UNION SELECT` swapping that attribute with NEW while keeping everything else from existing tuples with the same `ssn`. This ensures that if `(ssn=1, phone=A, email=X)` exists and we insert `(ssn=1, phone=B, email=Y)`, the cross-product tuples `(ssn=1, phone=A, email=Y)` and `(ssn=1, phone=B, email=X)` are also inserted.

#### 4.4b FD/CFD Enforcement

Templates: `cfd_check.sql.j2`, `fd_check.sql.j2`

All 3 FDs are guarded (wrapped in `\select_{defined(...)}` conditions), making them Conditional Functional Dependencies. This is where the **guard hierarchy** enters the pipeline.

**Guard hierarchy construction** (`build_guard_hierarchy` in `guard.py:78-129`):

1. Extract guard attributes from each target table's `SelectNode` conditions
2. Group tables by their guard set (as a frozen set of defined-attributes)
3. Sort groups by cardinality ascending
4. Build cumulative not-null/null column partitions

For this example, the hierarchy has 3 levels:

| Level | Guard Attributes | Target Tables | Not-Null Cols | Null Cols |
|---|---|---|---|---|
| 0 | (none) | Person, PersonPhone, PersonEmail | (none) | empid, name, hdate, phone, email, dept, manager |
| 1 | empid, hdate | Employee, EmployeeDate | empid, hdate | name, phone, email, dept, manager |
| 2 | empid, hdate, dept, manager | PED, PEDDept, DeptManager | empid, hdate, dept, manager | name, phone, email |

The hierarchy encodes that a level-2 tuple (all of empid, hdate, dept, manager defined) is strictly more informative than level-1 (only empid, hdate defined), which is more informative than level-0 (no optional information).

**CFD branch generation** (`build_cfd_where_branches` in `guard.py:132-192`):

Each CFD produces exhaustive WHERE branches covering all null-pattern states that would violate the dependency:

**CFD 1** (`empid -> hdate`, guards: `{empid, hdate}`):
1. Main FD violation: `R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R1.empid = R2.empid AND R1.hdate <> R2.hdate`
2. Incoherence: `R2.empid IS NOT NULL AND R2.hdate IS NULL` (can't have empid without hdate)
3. Incoherence: `R2.empid IS NULL AND R2.hdate IS NOT NULL` (can't have hdate without empid)

**CFD 2** (`empid -> dept`, guards: `{empid, hdate, dept, manager}`): Cross-level — LHS `empid` belongs to level 1, RHS `dept` to level 2. Additional branches enforce:
- `empid IS NULL AND dept IS NOT NULL` (can't have dept without empid)
- `empid IS NULL AND manager IS NOT NULL` (can't have manager without empid)
- Intra-level coherence: `dept IS NOT NULL AND manager IS NULL` and vice versa

**CFD 3** (`dept -> manager`, guards: `{empid, hdate, dept, manager}`): Same-level (both at level 2). Branches for intra-group coherence between dept and manager.

#### 4.4c INC Enforcement

Template: `inc_check.sql.j2`

The source `inc⊆_{manager, empid}` is an intra-table inclusion dependency — both sides reference `person_source`. The generated BEFORE INSERT trigger:

1. Short-circuits if `manager IS NULL` (NULL satisfies any inclusion)
2. Short-circuits if `manager = ssn` (self-reference via PK — the person is their own manager)
3. Checks `NEW.manager` exists in the `empid` column of `person_source` via `SELECT DISTINCT ... EXCEPT`

### 4.5 Tracking Infrastructure

Templates: `tracking_table.sql.j2`, `capture_function.sql.j2`, `capture_trigger.sql.j2`

For every table (9) in both contexts, and for both INSERT and DELETE events, three objects are generated:

1. **Tracking table** (`_<table>_INSERT` / `_<table>_DELETE`): empty clone via `SELECT * WHERE 1<>1`
2. **Capture function**: AFTER trigger that copies NEW/OLD into the tracking table, guarded by `_loop`
3. **Capture trigger**: wires the function to the base table

**Loop guard logic:**
- Source capture functions check `loop_start = -1`. If found, the capture is suppressed.
- Target capture functions check `loop_start = 1`. If found, the capture is suppressed.

This prevents infinite cascading: when a source change propagates to the target, the target-side capture sees the loop sentinel and no-ops.

**Output:** 36 SQL statements (9 tables x 2 events x 3 objects).

### 4.6 Join Staging

Templates: `tracking_table.sql.j2`, `join_function.sql.j2`, `join_trigger.sql.j2`

For each table and event, three more objects:

1. **Join staging table** (`_<table>_INSERT_JOIN` / `_<table>_DELETE_JOIN`): another empty clone
2. **Join function**: natural-joins tracked rows with other context tables to reconstruct universal tuples, then inserts a sentinel into `_loop`
3. **Join trigger**: fires when rows appear in the tracking table

**Source side (1 table):** The join function for `person_source_INSERT` is trivial — the source table already contains all universal attributes, so it simply copies the tracked row. No join is needed. (See [layers.md](../architecture/layers.md) for why this degenerates.)

**Target side (8 tables):** Each join function natural-joins the tracked table's staging rows with all other 7 target tables via `NATURAL LEFT OUTER JOIN`. For example, `person_INSERT`'s join function joins:

```sql
SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
FROM _person_INSERT_JOIN
NATURAL LEFT OUTER JOIN _personphone
NATURAL LEFT OUTER JOIN _personemail
NATURAL LEFT OUTER JOIN _employee
...
NATURAL LEFT OUTER JOIN _deptmanager
```

The LEFT OUTER JOIN (rather than inner join) preserves tuples where nullable attributes are absent.

**Output:** 36 more SQL statements.

### 4.7 Mapping Functions

Templates: `insert_mapping.sql.j2`, `delete_mapping.sql.j2`, `mapping_trigger.sql.j2`

Four bidirectional mapping functions form the core of the transducer.

#### SOURCE_INSERT_FN (source -> target)

When a row is inserted into `person_source`, the tracked universal tuple is projected into target tables. For each target table:

- **Unguarded tables** (Person, PersonPhone, PersonEmail): direct `INSERT ... SELECT DISTINCT <attrs> FROM _person_source_INSERT_JOIN WHERE <pk_cols> IS NOT NULL` with `ON CONFLICT DO NOTHING`
- **Guarded tables** (Employee through DeptManager): wrapped in `IF EXISTS (... WHERE guard_check)` — only populate if the guard condition is met. For example, Employee is only populated if `empid IS NOT NULL AND hdate IS NOT NULL`.

Trigger: fires on INSERT into `_person_source_INSERT_JOIN`.

#### TARGET_INSERT_FN (target -> source)

The most complex function. When target tables change:

1. **Create temp table** with universal columns
2. **Natural-join** all 8 target `_INSERT_JOIN` tables
3. **Null-pattern filter** (`build_null_pattern_where`): only valid specialization patterns are kept:
   ```sql
   WHERE ssn IS NOT NULL AND (
       (empid IS NULL AND name IS NULL AND hdate IS NULL AND ...)  -- level 0
       OR (empid IS NOT NULL AND ... AND hdate IS NOT NULL AND ...) -- level 1
       OR (empid IS NOT NULL AND ... AND dept IS NOT NULL AND ...)  -- level 2
   )
   ```
4. **Containment pruning** (`build_containment_pruning`): if a level-2 tuple exists for the same identity (`ssn`), delete less-informative level-0 or level-1 tuples from `temp_table_join`
5. **Insert** into `person_source` with `ON CONFLICT DO NOTHING`
6. **Loop sentinel**: `INSERT INTO _loop VALUES (-1)` — suppresses the re-triggered source capture
7. **Cleanup**: truncate all 16 staging tables + drop the temp table

Triggers: one per target table (8 total), all firing on INSERT into `_<table>_INSERT_JOIN`.

#### SOURCE_DELETE_FN (source -> target)

When a source tuple is deleted, the mapping must decide **which** target rows to remove. MVD independence checks prevent over-deletion:

For each MVD (phone, email), the function checks whether other source tuples with the same `ssn` still reference the same phone/email value. If so, the corresponding target table row is preserved.

A **full independence check** handles the case where no tuples remain with the same PK — all target tables are cleaned.

#### TARGET_DELETE_FN (target -> source)

Joins source base tables (not target) to check whether removing a universal tuple leaves other source tuples that still require the same data. Uses `IS NOT DISTINCT FROM` for nullable column comparisons.

---

## Output Summary

The full compilation from 3 input files (~45 lines) produces ~1,950 lines of PostgreSQL:

| Section | Object count | Purpose |
|---|---|---|
| Preamble | 3 statements | Schema, loop table |
| Base tables | 9 CREATE TABLE | Data storage |
| Foreign keys | 7 ALTER TABLE | Inter-table referential integrity |
| Constraints | ~10 functions + triggers | MVD, CFD, INC enforcement |
| Tracking | 36 (18 tables + 18 fn/trigger pairs) | Change capture with loop guard |
| Join staging | 36 (18 tables + 18 fn/trigger pairs) | Universal tuple reconstruction |
| Mapping | 4 functions + ~17 triggers | Bidirectional synchronization |

## Data Flow Diagram

```
universal.json ──┐
                 ├──> Context.from_file(SOURCE) ──> Context(1 table, 7 deps)
source.txt ──────┘                                         │
                                                           ├──> TransducerContext
universal.json ──┐                                         │
                 ├──> Context.from_file(TARGET) ──> Context(8 tables, 16 deps)
target.txt ──────┘                                         │
                                                           v
                                                    Generator.compile()
                                                           │
                          ┌─── _preamble() ────────────────┤ DROP/CREATE schema + _loop
                          ├─── _base_tables() ─────────────┤ 9 CREATE TABLE
                          ├─── _foreign_keys() ────────────┤ 7 ALTER TABLE ADD FK
                          ├─── _constraints() ─────────────┤ MVD check+grounding, 3 CFDs, 1 INC
                          ├─── _tracking() ────────────────┤ 18 tracking tables + 18 fn/triggers
                          ├─── _join() ────────────────────┤ 18 join tables + 18 fn/triggers
                          └─── _mapping() ─────────────────┤ 4 mapping fns + ~17 triggers
                                                           │
                                                           v
                                                    ~1,950 lines of PostgreSQL
```

## Runtime Trigger Chain

When a row is inserted into source table `person_source`, the following chain executes:

```
INSERT INTO _person_source
  │
  ├─> BEFORE INSERT: MVD check (reject if violates mvd_{ssn,phone/email})
  ├─> BEFORE INSERT: CFD checks x3 (reject if violates fd_{empid,hdate}, etc.)
  ├─> BEFORE INSERT: INC check (reject if manager not in empid)
  │
  ├─> AFTER INSERT: MVD grounding (insert complementary tuples for 4NF)
  ├─> AFTER INSERT: source_person_source_INSERT_fn()
  │     └─> Copies NEW into _person_source_INSERT (if _loop allows)
  │           └─> AFTER INSERT trigger on _person_source_INSERT:
  │                 └─> Join function: copies to _person_source_INSERT_JOIN
  │                       └─> Inserts loop sentinel (+1)
  │                       └─> AFTER INSERT trigger on _person_source_INSERT_JOIN:
  │                             └─> SOURCE_INSERT_FN():
  │                                   ├─> INSERT INTO _person (ssn, name)
  │                                   ├─> INSERT INTO _personphone (ssn, phone)
  │                                   ├─> INSERT INTO _personemail (ssn, email)
  │                                   ├─> IF guard: INSERT INTO _employee ...
  │                                   ├─> IF guard: INSERT INTO _employeedate ...
  │                                   ├─> IF guard: INSERT INTO _ped ...
  │                                   ├─> IF guard: INSERT INTO _peddept ...
  │                                   ├─> IF guard: INSERT INTO _deptmanager ...
  │                                   └─> Cleanup: DELETE FROM tracking/join tables + _loop
  │
  │   (Each target INSERT triggers target capture, but _loop sentinel
  │    suppresses it, breaking the cycle.)
```
