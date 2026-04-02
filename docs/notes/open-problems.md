# Open Problems

This document compiles unresolved issues identified during the design and prototyping of the transducer compiler. Each problem represents a gap between the current prototype SQL and a fully general, automated compilation pipeline. They are listed roughly in order of how early they arise in the compilation process: join construction, constraint enforcement, update propagation, and schema structure.

---

## NATURAL JOIN ordering

The join layer reconstructs full tuples by NATURAL JOINing all base tables together, but no formal algorithm exists for determining a correct join order. When two tables share no attributes, joining them produces a cartesian product, inflating the result set and breaking downstream projections. The correct order must follow the foreign key graph so that each successive join shares at least one attribute with the accumulated result.

This matters because the join layer is central to the transducer architecture. Every INSERT and DELETE propagation passes through a join function that projects the full join into individual target tables. If the join order is wrong, the projected tuples are nonsensical cartesian combinations rather than meaningful data. Since the compiler must generate these join functions automatically, it needs a deterministic algorithm to walk the FK graph and produce a valid ordering.

Currently the prototype uses a manually chosen join order that works for the running example. No automated method has been developed. A graph traversal starting from a root table and visiting neighbors via shared attributes is the likely direction, but it has not been formalized or implemented.

Source: `notes/transducer_definition.sql` lines 194-215

## Non-shared-LHS MVDs

The MVD constraint check query assumes all multivalued dependencies in a table share the same left-hand side. For example, if a table has X ->> Y1 and X ->> Y2, the EXCEPT-based violation check correctly verifies that all cross-product combinations exist. However, when MVDs have different left-hand sides (e.g., X ->> Z and XY ->> T), the same EXCEPT query produces false violations because the cross-product logic does not account for the differing determinant sets.

This is a problem for the compiler because MVD enforcement is generated automatically from the parsed constraint declarations. If the compiler encounters a table with non-shared-LHS MVDs, it will emit incorrect violation check SQL that rejects valid insertions. The compiler either needs to detect this case and refuse it, or generate a different form of check query.

No solution exists yet. The current implementation only handles the shared-LHS case and does not detect or warn about the non-shared-LHS situation.

Source: `notes/constraint_definition.sql` lines 210-216

## Join layer optimization

Each join function in the current prototype computes the same full NATURAL LEFT OUTER JOIN query once for every target table it projects into. If a source schema has n target tables, the identical multi-table join is evaluated n times within a single trigger invocation. This is redundant work that scales poorly as the number of target tables grows.

The compiler needs to eliminate this redundancy to produce efficient trigger functions. The natural solution is to compute the join once into a temporary table and then project from that temp table into each target. The current prototype already uses temporary tables in some places, so the pattern exists, but it has not been formalized into a general compilation rule.

Current status: the problem is identified and the solution direction is clear (single temp table per join invocation), but the compiler does not yet implement this optimization.

Source: `notes/transducer_definition.sql` lines 316-317

## DELETE independence generalization

When a DELETE occurs in a source table, the transducer must determine which target table tuples are no longer needed. The current approach uses an EXCEPT query to check whether other source tuples still reference the same attribute values. If no other tuple provides those values, the target tuple can be safely deleted. This per-target-table independence check works correctly for the running example.

However, the running example has a relatively simple FK structure. For schemas with deeper foreign key hierarchies, multiple connected components, or tables that participate in several FK relationships simultaneously, the EXCEPT-based check may miss dependencies or produce incorrect results. The independence property needs formal validation against more complex schema topologies.

Current status: the approach works empirically for the prototype example but has not been tested or proven correct for the general case.

Source: `notes/updates_and_more.sql`

## Composite PK foreign keys

PostgreSQL does not allow creating a foreign key that references a subset of a composite primary key. For example, if `_EMPDEP` has a composite primary key `(ssn, phone, email)`, then `_PERSON_CAR.ssn REFERENCES _EMPDEP.ssn` is rejected because `ssn` alone is not a unique key in `_EMPDEP`. This is a fundamental SQL limitation, not a bug.

This matters for the compiler because inclusion dependencies between tables frequently involve attributes that are part of a composite primary key in the referenced table. The compiler cannot rely on native PostgreSQL REFERENCES constraints for these cases and must instead generate custom trigger functions that enforce the inclusion dependency manually, similar to how other constraint types are already handled.

Current status: the problem is identified and the workaround (custom inclusion dependency triggers) is known, but these triggers have not been implemented.

Source: `notes/output.sql` lines 24-31

## Disconnected table graphs

When a schema contains multiple independent connected components (for example, tables T1-T11 forming one group, T12-T15 another, and T16 standing alone), performing a full NATURAL JOIN across all tables produces massive cartesian products between the unconnected groups. This is a specific instance of the join ordering problem but with a different solution: rather than finding a single correct order, the compiler must identify connected components and restrict joins to within each component.

This is critical for any non-trivial schema. Real-world databases routinely contain independent table groups, and a naive full join across all of them would be computationally explosive. The compiler must partition the table set by connectivity before generating join functions.

Current status: the need for connected component detection is identified. The grouping must be determined from the FK graph and encoded into the generated SQL so that each join function only joins tables within its own component.

Source: `notes/transducer_definition.sql` lines 199-215

## Tuple containment in target-to-source mapping with NULLs

When nullable attributes are present in a URA (Universal Relation Assumption) schema, the target-to-source mapping function's NATURAL LEFT OUTER JOIN of `_INSERT_JOIN` tables can produce multiple valid but overlapping tuples for the same entity. The less-informative tuples (those with more NULLs) must be pruned before inserting into the source table, or PK violations occur.

For example, inserting an employee `{ssn5, emp5, Jex, hdate5, phone51, mail51, NULL, NULL}` into target tables and then reconstructing via the join produces two tuples:

```
{ssn5, emp5, Jex, hdate5, phone51, mail51, NULL, NULL}   -- correct, most informative
{ssn5, NULL, Jex, NULL,   phone51, mail51, NULL, NULL}    -- correct but dominated
```

Both satisfy the WHERE clause's null-pattern filter (which allows `empid IS NULL AND hdate IS NULL` OR `empid IS NOT NULL AND hdate IS NOT NULL`), but inserting both violates the PK constraint on `(ssn, phone, email)`.

The current workaround uses a manual containment check:

```sql
IF EXISTS (SELECT * FROM temp_table_join
         EXCEPT (SELECT * FROM temp_table_join WHERE empid IS NULL)) THEN
   IF EXISTS (SELECT * FROM temp_table_join
         EXCEPT (SELECT * FROM temp_table_join WHERE dept IS NULL)) THEN
      DELETE FROM temp_table_join WHERE dept IS NULL;
   ELSE
      DELETE FROM temp_table_join WHERE empid IS NULL;
   END IF;
END IF;
```

This approach is manually tailored to the known hierarchy (person ⊃ employee ⊃ employee-with-department) and does not generalize. The compiler needs a systematic way to detect and resolve tuple containment based on the schema's null-pattern structure. A general algorithm would need to determine, from the set of guard dependencies and conditional FDs, which null-pattern groups dominate others and prune accordingly.

Current status: a hand-written workaround exists for the PERSON example (`docs/notes/example/null_example_notes.sql`). No general algorithm has been developed.

Source: `docs/notes/example/null_example_notes.sql` lines 269-297

## Inclusion dependencies

Inclusion dependencies generalize foreign keys: they state that the set of values appearing in one attribute must be a subset of the values appearing in another attribute, possibly in a different table. For example, a `manager` attribute must contain only values that also appear in the `ssn` attribute. The constraint definition notes mention this dependency type but provide no implementation detail.

The compiler needs to support inclusion dependencies because they appear in the relational algebra constraint declarations (as `inc=` and `inc⊆` operators in RAPT2). Without trigger functions to enforce them, the generated database would silently allow violations of these constraints. The implementation should follow the same pattern as other constraint triggers: a BEFORE INSERT check that raises an exception if the inclusion is violated.

Current status: a working implementation exists for the intra-table case in `docs/notes/example/1_source.sql` (`check_PERSON_IND_FN_1`), which enforces `manager ⊆ ssn` within the same table using a BEFORE INSERT trigger. The function handles NULLs (allowing NULL manager) and self-reference (allowing `manager = ssn`). The RAPT2 parser already recognizes inclusion dependency nodes, but the compiler does not yet generate these enforcement triggers automatically. The inter-table case (INC across different tables) has not been implemented.

Source: `notes/constraint_definition.sql` lines 436-444, `docs/notes/example/1_source.sql` lines 71-93
