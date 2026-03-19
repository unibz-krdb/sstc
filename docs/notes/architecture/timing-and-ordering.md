# Timing and Ordering

SQL transactions execute operations strictly sequentially, and PostgreSQL triggers fire in a deterministic order. This creates constraints on how the transducer must order its INSERT and DELETE operations, how NATURAL JOINs must be sequenced, and how the final mapping function knows when all intermediate triggers have completed.

---

## INSERT Ordering

INSERT operations must respect foreign key constraints. Parent tables must be populated before child tables that reference them. In a schema with inclusion dependencies like `ind^=[PERSON_NAME, PERSON_PHONE](ssn, ssn)`, the table that is referenced must be inserted into first.

For a hierarchy like:

```
                [T1]
                /  \
            [T2]  [T3] -- [T4]
            /  \
        [T5]  [T6]
                 |
                [T7]
                /  \
            [T8]  [T9] - [T10] -- [T11]
```

The INSERT order follows from three observations:

1. **Top-down traversal**: Start INSERTs from the root of the dependency graph (T1) and work downward (T1, T2, T3, ...).
2. **MVD decompositions last**: Tables created from MVD decomposition (e.g., T4, T11) have composite keys and depend on other tables. They go at the end of the INSERT order.
3. **FK direction matters**: If T10's primary key is a foreign key of T9, then T9 depends on T10. T10 must be inserted before T9.

For the person example, a valid INSERT order is:

```
INSERT: PERSON_NAME, PERSON_PHONE, PERSON_EMAIL, EMPLOYEE
```

The INSERT order is less strict than the NATURAL JOIN order (see below). An incorrect INSERT order will cause the transaction to fail due to FK violations, but no data corruption occurs -- the transaction is simply rolled back.

## DELETE Ordering

DELETE order is the reverse of INSERT order. Child tables must be emptied before parent tables to avoid FK violations:

```
DELETE: EMPLOYEE, PERSON_EMAIL, PERSON_PHONE, PERSON_NAME
```

## NATURAL JOIN Ordering

The order of tables in the NATURAL LEFT OUTER JOIN query used by the [join layer](layers.md) is critical and more constrained than INSERT/DELETE ordering.

The key problem: **joining two tables that share no attributes produces a cartesian product**. This generates incorrect and massively inflated results. The join order must follow the FK graph so that each successive join shares at least one attribute with the accumulated result.

For the tree example above, a valid NJ order starting from T11 would be:

```sql
SELECT * FROM T11_INSERT
NATURAL LEFT OUTER JOIN T10
NATURAL LEFT OUTER JOIN T9
...
NATURAL LEFT OUTER JOIN T1
```

Each table in the sequence must share at least one attribute with some table already in the join. There is no formal algorithm for finding a correct NJ order yet -- it requires manual curation based on the schema's FK graph.

For schemas with disconnected components (multiple independent groups of tables), the NJ query must be restricted to tables within the same connected component. Joining across disconnected components produces cartesian products. The solution is to pre-define table groups and limit each NJ query to its group.

## The Wait Mechanism

The final mapping function (`SOURCE_INSERT_FN`) must only fire after ALL per-table join triggers have completed. If it fires too early, some SIJ tables will be empty and the mapping will produce incomplete results.

The mechanism uses the `_LOOP` table (see [loop-prevention.md](loop-prevention.md)) as a counter. Each per-table trigger (both the base INSERT trigger and the join-layer trigger) adds a row to `_LOOP`. The final mapping function checks whether the expected number of rows is present:

```sql
IF ABS(loop_start) = COUNT(*) FROM _LOOP
```

This condition ensures the function only proceeds when every trigger in the chain has fired and recorded its row. Until then, the function returns NULL and does nothing. When the last trigger fires and adds the final row to `_LOOP`, the count matches, and the mapping function executes.

This works because PostgreSQL triggers are deterministic within a transaction -- each trigger completes before the next one fires, and the `_LOOP` row count monotonically increases.

## Multi-Table DELETE Transactions

DELETE transactions involving multiple tables require special handling. Because the wait mechanism counts `_LOOP` rows, and each DELETE trigger adds a row, the system needs to know in advance how many tables will be affected.

The solution is to pre-seed `_LOOP` with a count value before the DELETE operations begin:

```sql
BEGIN
INSERT INTO _loop VALUES (4);  -- expecting deletes from 3 tables (4 = 3 + 1 pre-seed)
DELETE FROM table1 WHERE ...;
DELETE FROM table2 WHERE ...;
DELETE FROM table3 WHERE ...;
END;
```

The pre-seeded value ensures the wait mechanism's count check reaches the correct total, allowing the final mapping function to fire at the right time. Without pre-seeding, the mapping function would fire after the first table's delete trigger completes, before the other tables have been processed.
