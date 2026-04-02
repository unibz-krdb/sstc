# Loop Prevention

The transducer propagates updates bidirectionally between equivalent schemas. Without a guard mechanism, an INSERT on the source side would propagate to the target, which would trigger a propagation back to the source, and so on indefinitely. The `_LOOP` control table prevents this by letting each side detect when an update originated from the opposite side.

---

## The _LOOP Control Table

```sql
CREATE TABLE _LOOP (loop_start INT NOT NULL);
```

This single-column table acts as a signaling mechanism between the source and target sides of the transducer. It holds integer values that encode which side initiated the current update.

## The Four Update Types

There are four types of update in the transducer:

1. **INSERT from source** -- inserts `1` into `_LOOP`, blocked by presence of `-1`
2. **INSERT from target** -- inserts `-1` into `_LOOP`, blocked by presence of `1`
3. **DELETE from source** -- inserts `1` into `_LOOP`, blocked by presence of `-1`
4. **DELETE from target** -- inserts `-1` into `_LOOP`, blocked by presence of `1`

Each update in a transaction adds a row to `_LOOP`. This means a transaction touching 3 source tables will produce 3 rows containing `1` in `_LOOP`.

## Source-Side INSERT Trigger

When a row is inserted into a source table Si, the trigger checks for `-1` in `_LOOP`. If found, this update originated from the target side and has looped back -- the trigger returns NULL to cancel the insert. Otherwise, it copies the new row into the tracking table and proceeds:

```sql
CREATE OR REPLACE FUNCTION Si_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM _LOOP WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO SIi VALUES(NEW.X, NEW.Y, ...);
   RETURN NEW;
END IF;
END;  $$;
```

Note: the trigger does **not** insert into `_LOOP` or clean it up. The loop marker is inserted by the JOIN function (Step 2 of the [insert chain](../sql-generation/insert-chain.md)), and cleanup happens in the final mapping function.

## Target-Side INSERT Trigger

The target side is the mirror image. It checks for `1` (source-originated updates) and returns NULL to break the loop:

```sql
CREATE OR REPLACE FUNCTION Tj_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM _LOOP WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   INSERT INTO TIj VALUES(NEW.X, NEW.Y, ...);
   RETURN NEW;
END IF;
END;  $$;
```

## How It Works Step by Step

1. User inserts a row into source table S1.
2. `S1_INSERT_FN()` checks `_LOOP` for `-1` -- finds none. Copies NEW into SI1.
3. The JOIN function fires, inserts `1` into `_LOOP`, and populates the JOIN tables.
4. The final mapping function fires, inserts into target tables T1..Tm, and cleans up `_LOOP`.
5. Each target table insert fires `Tj_INSERT_FN()`, which checks `_LOOP` for `1` -- finds it. Returns NULL, canceling the insert. The loop stops.

## Cleanup in the Final Mapping Function

Even when the loop detection stops the back-propagation, the `_LOOP` table is also explicitly cleaned in the final mapping function `SOURCE_INSERT_FN()` to ensure no stale values remain:

```sql
CREATE OR REPLACE FUNCTION SOURCE_INSERT_FN()
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

DELETE FROM LOOP;

RETURN NEW;
END;  $$;
```

The `DELETE FROM LOOP` at the end ensures the control table is empty before the next transaction begins. This cleanup is critical -- leftover values would cause the next legitimate update to be incorrectly blocked.

## Dual Role of _LOOP

The `_LOOP` table serves a second purpose beyond loop prevention: it acts as a synchronization counter for the [wait mechanism](timing-and-ordering.md) that ensures the final mapping function only fires after all per-table join triggers have completed. Each trigger adds a row to `_LOOP`, and the final mapping function checks that the row count matches the expected number of triggers before proceeding.
