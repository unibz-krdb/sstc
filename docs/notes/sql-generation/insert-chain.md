# INSERT Trigger Chain

When a row is inserted into any base table, a 4-step trigger chain propagates the change through tracking tables, reconstructs a universal join, and maps the data to the opposite schema side. The chain uses the `_LOOP` table for synchronization and cycle prevention so that insertions flowing back from the opposite side do not re-trigger the chain.

---

## Step 1 -- Base INSERT trigger (`_INSERT_fn`)

Fires AFTER INSERT on the base table. Checks the `_LOOP` table for the opposite side's loop marker -- if present, the insert was triggered by the mapping function from the other side, so it returns NULL to break the cycle. Otherwise, copies the new row into the `_INSERT` tracking table.

Source-side tables check for `loop_start = -1` (target's marker):

```sql
CREATE OR REPLACE FUNCTION schema._tablename_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * FROM schema._loop WHERE loop_start = -1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO schema._tablename_INSERT VALUES(NEW.col1, NEW.col2, ...);
      RETURN NEW;
   END IF;
END; $$;

CREATE TRIGGER schema._tablename_INSERT_trigger
AFTER INSERT ON schema._tablename
FOR EACH ROW
EXECUTE FUNCTION schema._tablename_INSERT_fn();
```

Target-side tables check for `loop_start = 1` (source's marker):

```sql
CREATE OR REPLACE FUNCTION schema._tablename_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * FROM schema._loop WHERE loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO schema._tablename_INSERT VALUES(NEW.col1, NEW.col2, ...);
      RETURN NEW;
   END IF;
END; $$;
```

Concrete source-side example:

```sql
CREATE OR REPLACE FUNCTION transducer._empdep_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._empdep_INSERT VALUES(NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.dep_name, NEW.dep_address);
      RETURN NEW;
   END IF;
END; $$;
```

Concrete target-side example:

```sql
CREATE OR REPLACE FUNCTION transducer._person_INSERT_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO transducer._person_INSERT VALUES(NEW.ssn, NEW.name, NEW.dep_name);
      RETURN NEW;
   END IF;
END; $$;
```

## Step 2 -- JOIN function (`_INSERT_JOIN_fn`)

Fires AFTER INSERT on the `_INSERT` tracking table. Reconstructs the universal relation by creating a temporary table with all attributes from all tables on the same schema side, then populates it via NATURAL LEFT OUTER JOIN starting from the `_INSERT` table and joining against every other base table on the same side.

### Generic template

```sql
CREATE OR REPLACE FUNCTION schema._tablename_INSERT_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   CREATE TEMPORARY TABLE temp_table (
       universal_col1 VARCHAR(100),
       universal_col2 VARCHAR(100),
       ...all universal attributes...
   );

   INSERT INTO temp_table (
       SELECT DISTINCT col1, col2, ...all universal cols...
       FROM schema._TABLENAME_INSERT
       NATURAL LEFT OUTER JOIN schema._OTHER_TABLE1
       NATURAL LEFT OUTER JOIN schema._OTHER_TABLE2
       ...
       WHERE col1 IS NOT NULL AND col2 IS NOT NULL AND ...
   );

   -- Project temp_table into ALL _INSERT_JOIN tables on this side
   INSERT INTO schema._TABLE1_INSERT_JOIN (SELECT col_a, col_b FROM temp_table);
   INSERT INTO schema._TABLE2_INSERT_JOIN (SELECT col_c, col_d FROM temp_table);
   ...

   -- Insert loop marker: 1 for source side, -1 for target side
   INSERT INTO schema._LOOP VALUES (1);  -- or (-1) for target

   -- Cleanup
   DELETE FROM temp_table;
   DROP TABLE temp_table;

   RETURN NEW;
END; $$;

CREATE TRIGGER _tablename_INSERT_JOIN_trigger
AFTER INSERT ON schema._tablename_INSERT
FOR EACH ROW
EXECUTE FUNCTION schema._tablename_INSERT_JOIN_fn();
```

### Concrete source-side example (`_empdep`)

The source schema has 2 tables (`_empdep`, `_position`) with 8 universal attributes:

```sql
CREATE OR REPLACE FUNCTION transducer._empdep_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   CREATE TEMPORARY TABLE temp_table (
       ssn VARCHAR(100),
       name VARCHAR(100),
       phone VARCHAR(100),
       email VARCHAR(100),
       dep_name VARCHAR(100),
       dep_address VARCHAR(100),
       city VARCHAR(100),
       country VARCHAR(100)
   );

   INSERT INTO temp_table (
       SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country
       FROM transducer._EMPDEP_INSERT
       NATURAL LEFT OUTER JOIN transducer._POSITION
       WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL
             AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
             AND city IS NOT NULL AND country IS NOT NULL
   );

   INSERT INTO transducer._empdep_INSERT_JOIN (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table);
   INSERT INTO transducer._LOOP VALUES (1);
   INSERT INTO transducer._position_INSERT_JOIN (SELECT dep_address, city, country FROM temp_table);

   DELETE FROM temp_table;
   DROP TABLE temp_table;
   RETURN NEW;
END; $$;
```

### Concrete target-side example (`_person`)

The target schema has 6 tables. Each JOIN function joins the `_INSERT` table against the other 5 base tables:

```sql
CREATE OR REPLACE FUNCTION transducer._person_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   CREATE TEMPORARY TABLE temp_table (
       ssn VARCHAR(100),
       phone VARCHAR(100),
       email VARCHAR(100),
       name VARCHAR(100),
       dep_name VARCHAR(100),
       dep_address VARCHAR(100),
       city VARCHAR(100),
       country VARCHAR(100)
   );

   INSERT INTO temp_table (
       SELECT DISTINCT ssn, phone, email, name, dep_name, dep_address, city, country
       FROM transducer._PERSON_INSERT
       NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
       NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
       NATURAL LEFT OUTER JOIN transducer._DEPARTMENT
       NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY
       NATURAL LEFT OUTER JOIN transducer._CITY_COUNTRY
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
             AND name IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
             AND city IS NOT NULL AND country IS NOT NULL
   );

   INSERT INTO transducer._person_phone_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
   INSERT INTO transducer._person_email_INSERT_JOIN (SELECT ssn, email FROM temp_table);
   INSERT INTO transducer._person_INSERT_JOIN (SELECT ssn, name, dep_name FROM temp_table);
   INSERT INTO transducer._department_INSERT_JOIN (SELECT dep_name, dep_address FROM temp_table);
   INSERT INTO transducer._department_city_INSERT_JOIN (SELECT dep_address, city FROM temp_table);
   INSERT INTO transducer._LOOP VALUES (-1);
   INSERT INTO transducer._city_country_INSERT_JOIN (SELECT city, country FROM temp_table);

   DELETE FROM temp_table;
   DROP TABLE temp_table;
   RETURN NEW;
END; $$;
```

Key points about Step 2:
- The `_INSERT` table is always the leftmost table in the join (the "starting" table)
- The joins are against the **base** tables (not _INSERT tables) on the same schema side -- this fills in the remaining universal attributes from existing data
- The NOT NULL WHERE clause filters out rows where the join could not reconstruct a complete universal tuple
- Every _INSERT_JOIN table on the same side receives its projected columns, even the triggering table's own _INSERT_JOIN table
- The loop marker value depends on which side: `1` for source, `-1` for target

## Step 3 -- Final mapping function (`source_insert_fn` / `target_insert_fn`)

Fires AFTER INSERT on each `_INSERT_JOIN` table. This is where the actual cross-schema mapping happens. A single function is shared by all `_INSERT_JOIN` tables on the opposite side -- e.g., all target `_INSERT_JOIN` triggers call `target_insert_fn`, which maps data to the source side.

### Wait mechanism

The function first checks if all JOIN functions have fired by comparing the count of `_LOOP` rows to the absolute value of each row. If not all tables have reported in, it returns NULL:

```sql
IF NOT EXISTS (SELECT * FROM schema._loop,
    (SELECT COUNT(*) AS rc_value FROM schema._loop) AS row_count
    WHERE ABS(loop_start) = row_count.rc_value) THEN
   RETURN NULL;  -- not all tables ready yet
```

This works because each JOIN function inserts exactly one row into `_LOOP` with the same value (e.g., all `1` or all `-1`). When all N tables have fired, there are N rows all with the same absolute value, and `ABS(loop_start) = N` is true.

### When ready: join and map

Once all tables have reported:
1. Create a temp table with the universal schema
2. NATURAL JOIN all _INSERT_JOIN tables from the triggering side
3. INSERT into each table on the **opposite** side, respecting FK order, with `ON CONFLICT DO NOTHING`
4. Insert the reverse loop marker (to prevent the opposite side's INSERT triggers from re-triggering)
5. Clean up all _INSERT, _INSERT_JOIN, and _LOOP tables

See [mapping-functions.md](mapping-functions.md) for the full function templates and concrete examples.

### Trigger wiring

One trigger per `_INSERT_JOIN` table, all calling the same mapping function:

```sql
CREATE TRIGGER target_insert__tablename_INSERT_JOIN_trigger
AFTER INSERT ON schema._tablename_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION schema.target_insert_fn();
```

Concrete example (target side has 6 tables, so 6 triggers):

```sql
CREATE TRIGGER target_insert__person_phone_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_phone_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_email_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_email_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_city_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_city_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__city_country_INSERT_JOIN_trigger
AFTER INSERT ON transducer._city_country_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();
```

## Step 4 -- Cleanup

Performed at the end of the mapping function (Step 3). All tracking and staging tables are emptied:

```sql
-- Clear all _INSERT tables on the triggering side
DELETE FROM schema._table1_INSERT;
DELETE FROM schema._table2_INSERT;
...

-- Clear all _INSERT_JOIN tables on the triggering side
DELETE FROM schema._table1_INSERT_JOIN;
DELETE FROM schema._table2_INSERT_JOIN;
...

-- Clear the loop table
DELETE FROM schema._loop;
```

## DELETE chain symmetry

The DELETE trigger chain follows the exact same 4-step structure:
- Step 1 uses `OLD` instead of `NEW`, checks the same loop marker, writes to `_DELETE` tables
- Step 2 creates the same universal temp table, joins from `_DELETE` table, projects into `_DELETE_JOIN` tables
- Step 3 uses the same wait mechanism, maps to opposite side with DELETE operations
- Step 4 cleans up `_DELETE`, `_DELETE_JOIN`, and `_LOOP` tables

The key difference is in the mapping function (Step 3), where independence checks determine which target tables can safely have rows removed. See [delete-chain.md](delete-chain.md) for the full DELETE chain details.
