# docs/notes Structured Documentation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create `docs/notes/` and `docs/papers/` with compiled reference material extracted from the raw `notes/` SQL files, organized for LLM consumption.

**Architecture:** Extract content from 6 SQL files and 1 PDF in `notes/` into 19 structured files across 6 directories under `docs/`. Each markdown file is self-contained with a summary paragraph and SQL examples. README indexes in each directory.

**Tech Stack:** Markdown, SQL code blocks, git

**Source material (all in `notes/`):**
- `transducer_definition.sql` — architecture theory and pseudocode
- `constraint_definition.sql` — constraint implementation patterns
- `desired_output.sql` — clean target compiler output (1005 lines)
- `output.sql` / `full_script_v5_tabletemp.sql` — earlier working version with DELETE support (~1240 lines)
- `updates_and_more.sql` — DELETE independence logic and test transactions
- `2407.07502v1.pdf` — the academic paper

**File format:** Every markdown file starts with `# Title`, one summary paragraph, `---`, then content. README indexes use `- [file](file) — description` format. No frontmatter.

---

### Task 1: Create directory structure and move PDF

**Files:**
- Create: `docs/papers/README.md`
- Move: `notes/2407.07502v1.pdf` → `docs/papers/2407.07502v1.pdf`
- Create: `docs/notes/README.md`
- Create: `docs/notes/architecture/README.md`
- Create: `docs/notes/constraints/README.md`
- Create: `docs/notes/sql-generation/README.md`
- Create: `docs/notes/example/README.md`

**Step 1: Create all directories**

```bash
mkdir -p docs/papers docs/notes/architecture docs/notes/constraints docs/notes/sql-generation docs/notes/example
```

**Step 2: Move the PDF**

```bash
git mv notes/2407.07502v1.pdf docs/papers/2407.07502v1.pdf
```

**Step 3: Write `docs/papers/README.md`**

```markdown
# Papers

Academic publications related to the Semantic SQL Transducer.

- [2407.07502v1.pdf](2407.07502v1.pdf) — "Understanding the Semantic SQL Transducer (extended version)", Abgrall & Franconi, 2024. Formalizes lossless bidirectional schema transformations and the transducer architecture.
```

**Step 4: Write `docs/notes/README.md`**

Top-level index linking to all subdirectories and the standalone open-problems file. Content derived from the compiled analysis produced earlier in this conversation.

```markdown
# Notes

Compiled reference material for the Semantic SQL Transducer Compiler. Extracted from raw `notes/` SQL files and organized for quick lookup.

See also: [papers/](../papers/) for the academic paper.

## Sections

- [architecture/](architecture/) — The three-layer transducer architecture, loop prevention, timing
- [constraints/](constraints/) — SQL implementations of FDs, MVDs, guard deps, conditional join deps
- [sql-generation/](sql-generation/) — Patterns for generating tables, trigger chains, and mapping functions
- [example/](example/) — The EMPDEP/POSITION running example with full reference SQL output
- [open-problems.md](open-problems.md) — Unsolved issues and areas needing further work
```

**Step 5: Write the four subdirectory README indexes**

`docs/notes/architecture/README.md`:
```markdown
# Architecture

The Semantic SQL Transducer's three-layer trigger-based architecture for bidirectional schema synchronization.

- [layers.md](layers.md) — Base tables, update tracking layer, and join layer
- [loop-prevention.md](loop-prevention.md) — The _LOOP table mechanism that prevents infinite trigger recursion
- [timing-and-ordering.md](timing-and-ordering.md) — INSERT/DELETE ordering for foreign keys, NATURAL JOIN ordering, and the wait mechanism
```

`docs/notes/constraints/README.md`:
```markdown
# Constraints

SQL trigger functions that enforce relational constraints not natively supported by PostgreSQL. Each constraint type has a BEFORE INSERT check function and, where needed, an AFTER INSERT grounding function.

- [functional-dependencies.md](functional-dependencies.md) — FD violation checks, overlapping FDs
- [multivalued-dependencies.md](multivalued-dependencies.md) — MVD violation checks and automatic tuple grounding
- [guard-dependencies.md](guard-dependencies.md) — Jointly-null attribute constraints
- [conditional-join-dependencies.md](conditional-join-dependencies.md) — FDs that only apply when covered attributes are non-null
```

`docs/notes/sql-generation/README.md`:
```markdown
# SQL Generation

Patterns for the SQL code the compiler must generate. Each file describes a category of generated SQL with the generic template and concrete examples.

- [table-creation.md](table-creation.md) — Base tables, _INSERT tables, _INSERT_JOIN tables (the empty-clone pattern)
- [insert-chain.md](insert-chain.md) — The full INSERT trigger chain: base → tracking → join → mapping
- [delete-chain.md](delete-chain.md) — DELETE propagation and the independence check for partial deletes
- [mapping-functions.md](mapping-functions.md) — The final source_insert_fn / target_insert_fn that map between schemas
```

`docs/notes/example/README.md`:
```markdown
# Example

The running example used across all documentation: a 2-table source schema (EMPDEP, POSITION) mapped bidirectionally to a 6-table target schema.

- [schema.md](schema.md) — Source and target schemas, constraints, mappings, and sample data
- [reference-output.sql](reference-output.sql) — Complete SQL output the compiler should generate for this example
```

**Step 6: Commit**

```bash
git add docs/
git commit -m "docs: create docs/notes and docs/papers directory structure with indexes"
```

---

### Task 2: Write architecture documentation

**Files:**
- Create: `docs/notes/architecture/layers.md`
- Create: `docs/notes/architecture/loop-prevention.md`
- Create: `docs/notes/architecture/timing-and-ordering.md`
- Source: `notes/transducer_definition.sql` lines 1-314

**Step 1: Write `layers.md`**

Content extracted from `transducer_definition.sql` lines 1-100 and 264-314. Covers:
- The three layers: base tables, update tracking (INSERT/DELETE), join layer (INSERT_JOIN/DELETE_JOIN)
- Why the join layer exists (partial updates problem)
- The NATURAL LEFT OUTER JOIN query that populates the join layer
- The projection step from join result into per-table join tables
- ASCII diagram of the full layer stack

Include the generic SQL templates:
- `Si_INSERT_FN()` — copies NEW into SIi
- `SIi_JOIN_INSERT_FN()` — NJ query + project into SIJ tables
- `SOURCE_INSERT_FN()` — final mapping from SIJ tables to target

**Step 2: Write `loop-prevention.md`**

Content from `transducer_definition.sql` lines 396-512. Covers:
- The `_LOOP(loop_start INT)` table
- Source side inserts `1`, checks for `-1`
- Target side inserts `-1`, checks for `1`
- Cleanup in the final mapping function (`DELETE FROM _LOOP`)
- Generic SQL for both source and target INSERT functions with loop checks

**Step 3: Write `timing-and-ordering.md`**

Content from `transducer_definition.sql` lines 406-445 and the compiled analysis. Covers:
- INSERT order must respect foreign keys (parent before child)
- DELETE order is the reverse
- NATURAL JOIN order matters — wrong order produces cartesian products
- The wait mechanism: `ABS(loop_start) = COUNT(*)` check in final functions
- The `INSERT INTO _LOOP VALUES (n)` pre-seeding for multi-table DELETE transactions

**Step 4: Commit**

```bash
git add docs/notes/architecture/
git commit -m "docs: add architecture documentation (layers, loop prevention, timing)"
```

---

### Task 3: Write constraints documentation

**Files:**
- Create: `docs/notes/constraints/functional-dependencies.md`
- Create: `docs/notes/constraints/multivalued-dependencies.md`
- Create: `docs/notes/constraints/guard-dependencies.md`
- Create: `docs/notes/constraints/conditional-join-dependencies.md`
- Source: `notes/constraint_definition.sql`

**Step 1: Write `functional-dependencies.md`**

Content from `constraint_definition.sql` lines 38-155. Covers:
- The generic FD check query: `WHERE R1.LHS = R2.LHS AND R1.RHS <> R2.RHS`
- How NEW is materialized as a subselect in the FROM clause
- Overlapping FDs: each gets its own trigger function, examples with 2 and 3 overlapping FDs
- The BEFORE INSERT trigger template

**Step 2: Write `multivalued-dependencies.md`**

Content from `constraint_definition.sql` lines 158-296. Covers:
- The MVD violation check (EXCEPT pattern)
- Single MVD: `SELECT DISTINCT R1.X, R1.Y, R2.Z FROM R1, R2 WHERE R1.X = R2.X EXCEPT SELECT * FROM R1`
- Multiple MVDs with shared LHS: keep all Yi from R1, take Z from R2
- Tuple grounding (AFTER INSERT): UNION of swapped projections minus existing, then INSERT the missing tuples
- The scaling pattern for n MVDs
- Composite primary key implications

**Step 3: Write `guard-dependencies.md`**

Content from `constraint_definition.sql` lines 298-388. Covers:
- Jointly-null attributes: Z and T must be NULL together or not at all
- Naive approach (check for one-null-one-not) and why it fails with overlapping guards
- Working approach: enumerate all valid null-patterns, check `NOT EXISTS` against them
- The grounding algorithm reference

**Step 4: Write `conditional-join-dependencies.md`**

Content from `constraint_definition.sql` lines 392-433. Covers:
- FDs that only hold when covered attributes are non-null
- The combined query: FD violation check + joint-null guard
- Example with person/employee hierarchy (empid, hdate, dep_name)

**Step 5: Commit**

```bash
git add docs/notes/constraints/
git commit -m "docs: add constraint documentation (FDs, MVDs, guards, CJDs)"
```

---

### Task 4: Write sql-generation documentation

**Files:**
- Create: `docs/notes/sql-generation/table-creation.md`
- Create: `docs/notes/sql-generation/insert-chain.md`
- Create: `docs/notes/sql-generation/delete-chain.md`
- Create: `docs/notes/sql-generation/mapping-functions.md`
- Source: `notes/desired_output.sql`, `notes/output.sql`, `notes/updates_and_more.sql`

**Step 1: Write `table-creation.md`**

Content derived from `desired_output.sql` structure. Covers:
- Per-table generation checklist (what the compiler emits for each table)
- Base table: `CREATE TABLE schema.name (columns, PK, FKs)`
- INSERT tracking: `CREATE TABLE schema.name_INSERT AS SELECT * FROM schema.name WHERE 1<>1`
- INSERT_JOIN: same pattern as INSERT tracking
- DELETE tracking and DELETE_JOIN: same patterns
- The `_LOOP` table creation
- Column type mapping from universal schema JSON

**Step 2: Write `insert-chain.md`**

Content from `desired_output.sql` and `output.sql` trigger functions. Covers:
- The 4-step trigger chain: base INSERT → tracking function → join function → mapping function
- Step 1: `_INSERT_fn()` — AFTER INSERT trigger, loop check, copy NEW into _INSERT table
- Step 2: `_INSERT_JOIN_fn()` — AFTER INSERT on _INSERT table, creates temp_table, NJ query, projects into all _INSERT_JOIN tables, inserts into _LOOP
- Step 3: Final mapping function — wait mechanism, NJ of all _INSERT_JOIN tables, INSERT INTO target tables with ON CONFLICT DO NOTHING
- Step 4: Cleanup — DELETE FROM all _INSERT, _INSERT_JOIN, and _LOOP tables
- Note that DELETE chain is symmetric (same structure with OLD instead of NEW, _DELETE tables)

**Step 3: Write `delete-chain.md`**

Content from `updates_and_more.sql` and `output.sql` delete functions. Covers:
- The independence problem: deleting a tuple that shares attributes with other tuples
- The independence check query per target table
- Generic pattern: `IF EXISTS (SELECT * FROM ALL_temp WHERE <other-table-attrs match> EXCEPT SELECT * FROM NEW_temp)`
- If result exists: only delete from that specific target table
- If no result: cascade delete to parent tables too
- Pre-seeding `_LOOP` with count for multi-table DELETE transactions
- Example transactions from `updates_and_more.sql`

**Step 4: Write `mapping-functions.md`**

Content from `desired_output.sql` lines 840-1005 and `output.sql` lines 446-483. Covers:
- `source_insert_fn()` (S→T): NJ of source _INSERT_JOIN tables, project to each target table with ON CONFLICT DO NOTHING
- `target_insert_fn()` (T→S): NJ of target _INSERT_JOIN tables, project to each source table with ON CONFLICT DO NOTHING
- The wait mechanism in detail: `ABS(loop_start) = COUNT(*)` ensures all join tables are populated
- Insert ordering within the mapping function (respect FK hierarchy)
- The `_LOOP VALUES (-1)` or `(1)` insert before target/source inserts
- Cleanup section at the end

**Step 5: Commit**

```bash
git add docs/notes/sql-generation/
git commit -m "docs: add SQL generation documentation (tables, insert chain, delete chain, mappings)"
```

---

### Task 5: Write example documentation and reference output

**Files:**
- Create: `docs/notes/example/schema.md`
- Create: `docs/notes/example/reference-output.sql`
- Source: `notes/desired_output.sql`, all other notes files for schema description

**Step 1: Write `schema.md`**

Covers:
- Source schema: _EMPDEP and _POSITION with all attributes, types, PKs, FKs
- Source constraints: 2 MVDs (ssn->>phone, ssn->>email), 2 FDs (dep_name→dep_address, city→country)
- Target schema: 6 tables with all attributes, PKs, FKs
- The lossless mappings in both directions (relational algebra from the paper)
- Sample data (from `output.sql` INSERT statements)
- ASCII diagram of the FK graph for both schemas

**Step 2: Write `reference-output.sql`**

Copy `notes/desired_output.sql` as-is into `docs/notes/example/reference-output.sql`. This is the canonical reference for what the compiler should produce. Add a header comment:

```sql
-- Reference SQL output for the EMPDEP/POSITION example.
-- This is the target output the SSTC compiler should generate.
-- Source: notes/desired_output.sql
```

**Step 3: Commit**

```bash
git add docs/notes/example/
git commit -m "docs: add example schema documentation and reference SQL output"
```

---

### Task 6: Write open-problems.md

**Files:**
- Create: `docs/notes/open-problems.md`
- Source: scattered comments across all notes files

**Step 1: Write `open-problems.md`**

Compile all unresolved issues mentioned in the notes:

1. **NATURAL JOIN ordering** — no formal algorithm for finding a correct order; incorrect order produces cartesian products (transducer_definition.sql lines 194-215)
2. **Non-shared-LHS MVDs** — the MVD constraint query only works when MVDs share the same LHS; no solution for `X ->> Z, XY ->> T` (constraint_definition.sql lines 210-216)
3. **Join layer optimization** — the same NJ query is computed n times for n join tables; needs deduplication (transducer_definition.sql lines 316-317)
4. **DELETE independence generalization** — the per-target-table independence check needs more testing for complex schemas (updates_and_more.sql lines 89-142)
5. **Composite PK foreign keys** — FK references to subsets of composite PKs don't work in SQL; requires custom inclusion dependency triggers (output.sql lines 24-31)
6. **Disconnected table graphs** — schemas with multiple independent connected components need group-restricted joins (transducer_definition.sql lines 199-215)
7. **Inclusion dependencies** — constraint_definition.sql mentions them but provides no implementation detail

**Step 2: Commit**

```bash
git add docs/notes/open-problems.md
git commit -m "docs: add open problems documentation"
```

---

### Task 7: Final review and cleanup commit

**Step 1: Verify all files exist**

```bash
find docs/ -type f | sort
```

Expected: 19 files (5 READMEs, 11 markdown content files, 1 SQL file, 1 PDF, 1 design doc)

**Step 2: Verify all README links are valid**

Read each README and confirm every linked file exists.

**Step 3: Final commit if any fixes needed**

```bash
git add docs/
git commit -m "docs: finalize docs/notes structure"
```
