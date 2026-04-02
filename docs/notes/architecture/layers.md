# The Three-Layer Architecture

The transducer architecture uses three layers of tables to propagate updates between equivalent database schemas. The base layer holds the actual data, the update tracking layer captures INSERTs and DELETEs as they happen, and the join layer reconstructs full tuples from partial updates so the mapping function always has complete data to work with.

---

## Layer 1: Base Tables

Let S and T be the source and target database schemas with tables S1, S2, ..., Sn and T1, T2, ..., Tm respectively. These are the actual database tables holding user data. Both schemas are assumed to be equivalent: every possible instance allowed in one schema exists in the other, and vice versa. This equivalence is what enables bidirectional updates.

## Layer 2: Update Tracking (INSERT/DELETE Tables)

For each base table Si, two tracking tables are created:

- **SIi** (INSERT table) -- holds NEW tuples added to Si
- **SDi** (DELETE table) -- holds OLD tuples removed from Si

By extracting update tuples into dedicated tables, we avoid expensive comparison operations between S and T to discover what changed. Each modification is directly inserted into the update layer as it happens.

### Si_INSERT_FN() -- AFTER INSERT trigger on base table

When a row is inserted into a base table Si, this trigger copies the new row into the corresponding INSERT tracking table SIi. It also checks the `_LOOP` table to prevent recursive updates from looping back (see [loop-prevention.md](loop-prevention.md)).

```sql
CREATE OR REPLACE FUNCTION Si_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO SIi VALUES(NEW.X, NEW.Y, ... );
   RETURN NEW;
END IF;
END;  $$;

CREATE TRIGGER Si_INSERT_TRIGGER
AFTER INSERT ON Si
FOR EACH ROW
EXECUTE FUNCTION Si_INSERT_FN();
```

The same pattern applies for DELETE triggers, which copy OLD tuples into SDi:

```sql
CREATE OR REPLACE FUNCTION Si_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   INSERT INTO SDi VALUES(OLD.X, OLD.Y, ... );
   RETURN NEW;
END IF;
END;  $$;

CREATE TRIGGER Si_DELETE_TRIGGER
AFTER DELETE ON Si
FOR EACH ROW
EXECUTE FUNCTION Si_DELETE_FN();
```

## Why the Update Layer Alone Is Not Enough: The Partial Updates Problem

The initial intuition is to use the INSERT tracking tables directly as the source for the mapping function:

```sql
INSERT INTO T1 (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);
...
INSERT INTO Tm (SELECT DISTINCT ATT1, ATT2, ... FROM SI1, SI2, ... WHERE ...);
```

This fails when a transaction only updates some source tables. Consider a table `PERSON(ssn, name, phone, department)` split into `PERSON_Name(ssn, name)`, `PERSON_Phone(ssn, phone)`, and `Employee(ssn, department)` with the MVD `ssn ->> phone`. Adding a new phone number for an existing person means inserting only into `PERSON_Phone` -- there is no corresponding insert into `PERSON_Name` because no new name data was added. If the mapping function only reads from INSERT tables, it will find `PERSON_NAME_INSERT` empty and fail to reconstruct the full tuple.

A decision-tree approach (checking which INSERT tables are populated and substituting base tables for empty ones) grows combinatorially with the number of tables and requires deep knowledge of the specific mapping:

```sql
IF EXISTS (SELECT * FROM PERSON_PHONE_INSERT) THEN
   IF EXISTS (SELECT * FROM PERSON_NAME_INSERT) THEN
      INSERT INTO PERSON (SELECT ssn, name, phone FROM PERSON_PHONE_INSERT, PERSON_NAME_INSERT);
   ELSE
      INSERT INTO PERSON (SELECT ssn, name, phone FROM PERSON_PHONE_INSERT, PERSON_NAME);
   END IF;
   RETURN NEW;
END IF;
RETURN NULL;
```

This motivates the third layer.

## Layer 3: Join Layer (INSERT_JOIN/DELETE_JOIN Tables)

The join layer solves the partial updates problem generically. For each INSERT tracking table SIi, a corresponding join table SIJi is created. These join tables are populated by a NATURAL LEFT OUTER JOIN query that starts from the INSERT table and joins with all other base tables in the schema, then projects the result into per-table join tables.

### The NATURAL LEFT OUTER JOIN Query

Starting from a populated INSERT table SIi, the query joins it with every other base table to reconstruct full tuples:

```sql
SELECT * FROM SIi
NATURAL LEFT OUTER JOIN S1
...
NATURAL LEFT OUTER JOIN Sn
```

The LEFT OUTER JOIN (rather than plain NATURAL JOIN) is necessary to handle nullable attributes. For example, if `department` is nullable, inserting a person without a department must produce a tuple with `department = NULL` rather than being dropped entirely.

The join is executed once per INSERT table that received data. When multiple INSERT tables receive data in the same transaction (e.g., both `PERSON_PHONE_INSERT` and `PERSON_EMAIL_INSERT`), each one independently runs the full NJ query. The union of results across all INSERT tables produces the complete set of tuples needed by the mapping. This is necessary because each INSERT table may produce different join results -- for example, with MVDs `ssn ->> phone` and `ssn ->> email`, inserting a new phone and a new email each independently generate different cross-product tuples.

**URA (single-table) schemas:** When the source side has only one table (a Universal Relation Assumption schema), Layer 3 in the S-to-T direction degenerates -- there are no other base tables to join against, so the NJ query is effectively an identity. The INSERT table's contents are copied directly into the temp table and then projected into the target-side `_INSERT_JOIN` tables. Layer 3 is still structurally required because the wait mechanism and projection step depend on it, but the join itself is trivial. The PERSON example in `docs/notes/example/` demonstrates this case: `_PERSON` is the sole source table, decomposing into 8 target tables. In the T-to-S direction, Layer 3 operates normally -- each of the 8 target tables joins against the other 7 base tables to reconstruct the universal tuple.

### The Projection Step

The NJ result is projected into each join-layer table. Each SIJk receives only the attributes belonging to table Sk:

```sql
CREATE OR REPLACE FUNCTION SIi_JOIN_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$

INSERT INTO SIJ1 (SELECT DISTINCT ATT1, ATT2 ... FROM SIi
   NATURAL LEFT OUTER JOIN S1
   ...
   NATURAL LEFT OUTER JOIN Sn);

INSERT INTO SIJ2 (SELECT DISTINCT ATT1, ATT2 ... FROM SIi
   NATURAL LEFT OUTER JOIN S1
   ...
   NATURAL LEFT OUTER JOIN Sn);

...

INSERT INTO SIJn (SELECT DISTINCT ATT1, ATT2 ... FROM SIi
   NATURAL LEFT OUTER JOIN S1
   ...
   NATURAL LEFT OUTER JOIN Sn);
RETURN NEW;
END;  $$;
```

### SOURCE_INSERT_FN() -- The Final Mapping Function

Once all join-layer tables are populated, the final mapping function reads from the SIJ tables and inserts into the target schema. It also cleans up all INSERT and JOIN tables afterward:

```sql
CREATE OR REPLACE FUNCTION SOURCE_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

INSERT INTO T1 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
INSERT INTO T2 (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);
...
INSERT INTO Tm (SELECT ATT1, ATT2, ... FROM SIJ1... WHERE ...);

DELETE FROM SI1;
DELETE FROM SI2;
...
DELETE FROM SIn;
DELETE FROM SIJ1;
...
DELETE FROM SIJN;
RETURN NEW;
END;  $$;
```

## Full Layer Stack

```
       S1          S2    ...    Sn                   (base tables)
      /   \       /   \       /   \
   SI1  SD1    SI2  SD2    SIn  SDn                  (update tracking layer)
    |    |      |    |      |    |
  SIJ1 SDJ1  SIJ2 SDJ2  SIJn SDJn                   (join layer)

  TIJ1 TDJ1  TIJ2 TDJ2  TIJm TDJm                  (join layer)
    |    |      |    |      |    |
   TI1  TD1    TI2  TD2    TIm  TDm                  (update tracking layer)
      \   /       \   /       \   /
       T1          T2    ...    Tm                   (base tables)
```

The data flow for an INSERT on the source side is: base table Si triggers copy into SIi, SIi triggers the NJ query which populates SIJ1..SIJn, then the final mapping function reads from SIJ tables and inserts into target tables T1..Tm. The same architecture applies symmetrically for updates originating from the target side, and for DELETE operations (using SD/SDJ tables instead of SI/SIJ).
