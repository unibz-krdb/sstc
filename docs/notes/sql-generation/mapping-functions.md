# Mapping Functions

The mapping functions (`source_insert_fn` and `target_insert_fn`) are the final stage of the INSERT trigger chain. They read from `_INSERT_JOIN` tables on one schema side, reconstruct the universal relation via NATURAL JOIN, then project and insert into each table on the opposite schema side. A single mapping function is shared by all `_INSERT_JOIN` tables on the triggering side, called by one trigger per table.

---

## source_insert_fn (S-to-T direction)

Reads from source `_INSERT_JOIN` tables, NATURAL JOINs them, and projects into each target table. Triggered by inserts into any source-side `_INSERT_JOIN` table.

### Wait mechanism

Before proceeding, checks that all source-side JOIN functions have fired by comparing `_LOOP` row count to the absolute value of each row:

```sql
IF NOT EXISTS (SELECT * FROM schema._loop,
    (SELECT COUNT(*) AS rc_value FROM schema._loop) AS row_count
    WHERE ABS(loop_start) = row_count.rc_value) THEN
   RETURN NULL;  -- not all tables ready yet
```

This returns NULL (and exits early) until all source tables have inserted their loop marker. Each source-side JOIN function inserts `VALUES (1)`, so when N source tables have fired, there are N rows each containing `1`, and `ABS(1) = N` becomes true.

### Generic template

```sql
CREATE OR REPLACE FUNCTION schema.source_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF NOT EXISTS (SELECT * FROM schema._loop,
    (SELECT COUNT(*) AS rc_value FROM schema._loop) AS row_count
    WHERE ABS(loop_start) = row_count.rc_value) THEN
   RETURN NULL;
ELSE
   -- INSERT into each target table (FK order, parents first)
   -- using NATURAL JOIN of source _INSERT_JOIN tables
   -- with ON CONFLICT (pk_cols) DO NOTHING

   -- Cleanup all source _INSERT, _INSERT_JOIN, _LOOP tables
   RETURN NEW;
END IF;
END; $$;
```

### Concrete example

From `output.sql` -- the source has 2 tables (`_empdep`, `_position`), the target has 6 tables:

```sql
CREATE OR REPLACE FUNCTION transducer.source_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF NOT EXISTS (SELECT * FROM transducer._loop,
    (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
    WHERE ABS(loop_start) = row_count.rc_value) THEN
   RETURN NULL;
ELSE
   -- Insert into target tables in FK order (parents first)
   INSERT INTO transducer._city_country (
       SELECT DISTINCT city, country
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (city) DO NOTHING;

   INSERT INTO transducer._department_city (
       SELECT DISTINCT dep_address, city
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (dep_address) DO NOTHING;

   INSERT INTO transducer._department (
       SELECT DISTINCT dep_name, dep_address
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (dep_name) DO NOTHING;

   INSERT INTO transducer._person (
       SELECT DISTINCT ssn, name, dep_name
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (ssn) DO NOTHING;

   INSERT INTO transducer._person_email (
       SELECT DISTINCT ssn, email
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (ssn, email) DO NOTHING;

   INSERT INTO transducer._person_phone (
       SELECT DISTINCT ssn, phone
       FROM transducer._EMPDEP_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._POSITION_INSERT_JOIN
       WHERE ssn IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL AND dep_address IS NOT NULL
   ) ON CONFLICT (ssn, phone) DO NOTHING;

   -- Cleanup
   DELETE FROM transducer._empdep_INSERT;
   DELETE FROM transducer._position_INSERT;
   DELETE FROM transducer._empdep_INSERT_JOIN;
   DELETE FROM transducer._position_INSERT_JOIN;
   DELETE FROM transducer._loop;

   RETURN NEW;
END IF;
END; $$;
```

### Triggers for source_insert_fn

One trigger per source-side `_INSERT_JOIN` table:

```sql
CREATE TRIGGER source_insert__empdep_INSERT_JOIN_trigger
AFTER INSERT ON transducer._empdep_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_insert_fn();

CREATE TRIGGER source_insert__position_INSERT_JOIN_trigger
AFTER INSERT ON transducer._position_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_insert_fn();
```

## target_insert_fn (T-to-S direction)

Reads from target `_INSERT_JOIN` tables, reconstructs the universal relation, and projects into each source table. The same pattern as `source_insert_fn` but in the opposite direction.

### Key differences from source_insert_fn

- Reads from **target** `_INSERT_JOIN` tables instead of source
- Inserts into **source** tables instead of target
- Inserts the **reverse loop value** before source inserts: `INSERT INTO _LOOP VALUES (-1)` (the source INSERT triggers check for `-1` to suppress re-triggering)
- Uses a temp table to join all target `_INSERT_JOIN` tables first, then projects from the temp table

### Concrete example

From `output.sql` -- the target has 6 tables, the source has 2:

```sql
CREATE OR REPLACE FUNCTION transducer.target_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN
SELECT count(*) INTO v_loop FROM transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop,
    (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
    WHERE ABS(loop_start) = row_count.rc_value) THEN
   RETURN NULL;
ELSE
   -- Create temp table with universal schema
   CREATE TEMPORARY TABLE temp_table_join (
       ssn VARCHAR(100),
       name VARCHAR(100),
       phone VARCHAR(100),
       email VARCHAR(100),
       dep_name VARCHAR(100),
       dep_address VARCHAR(100),
       city VARCHAR(100),
       country VARCHAR(100)
   );

   -- JOIN all target _INSERT_JOIN tables
   INSERT INTO temp_table_join (
       SELECT DISTINCT ssn, name, phone, email, dep_name, dep_address, city, country
       FROM transducer._CITY_COUNTRY_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_CITY_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._DEPARTMENT_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._PERSON_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
       NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
       WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL
             AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
             AND city IS NOT NULL AND country IS NOT NULL
   );

   -- INSERT into source tables (FK order)
   INSERT INTO transducer._position (SELECT dep_address, city, country FROM temp_table_join)
       ON CONFLICT (dep_address) DO NOTHING;
   INSERT INTO transducer._loop VALUES (-1);
   INSERT INTO transducer._empdep (SELECT ssn, name, phone, email, dep_name, dep_address FROM temp_table_join)
       ON CONFLICT (ssn, phone, email) DO NOTHING;

   -- Cleanup all target _INSERT tables
   DELETE FROM transducer._person_phone_INSERT;
   DELETE FROM transducer._person_email_INSERT;
   DELETE FROM transducer._person_INSERT;
   DELETE FROM transducer._department_INSERT;
   DELETE FROM transducer._department_city_INSERT;
   DELETE FROM transducer._city_country_INSERT;

   -- Cleanup all target _INSERT_JOIN tables
   DELETE FROM transducer._person_phone_INSERT_JOIN;
   DELETE FROM transducer._person_email_INSERT_JOIN;
   DELETE FROM transducer._person_INSERT_JOIN;
   DELETE FROM transducer._department_INSERT_JOIN;
   DELETE FROM transducer._department_city_INSERT_JOIN;
   DELETE FROM transducer._city_country_INSERT_JOIN;

   -- Cleanup loop and temp
   DELETE FROM transducer._loop;
   DELETE FROM temp_table_join;
   DROP TABLE temp_table_join;

   RETURN NEW;
END IF;
END; $$;
```

### Triggers for target_insert_fn

One trigger per target-side `_INSERT_JOIN` table (6 in this example):

```sql
CREATE TRIGGER target_insert__person_phone_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_phone_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_city_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_city_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__department_INSERT_JOIN_trigger
AFTER INSERT ON transducer._department_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_email_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_email_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__city_country_INSERT_JOIN_trigger
AFTER INSERT ON transducer._city_country_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();

CREATE TRIGGER target_insert__person_INSERT_JOIN_trigger
AFTER INSERT ON transducer._person_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_insert_fn();
```

## NATURAL JOIN ordering in the mapping function

The NATURAL JOIN of `_INSERT_JOIN` tables follows the same considerations as the join layer (Step 2):
- The starting table in the join is typically the table with the most attributes or the one that anchors the FK hierarchy
- In `target_insert_fn`, the join starts from `_CITY_COUNTRY_INSERT_JOIN` and walks up through the FK chain
- In `source_insert_fn`, each INSERT does its own inline join of the source `_INSERT_JOIN` tables

## INSERT ordering within the mapping

The INSERT statements into the target/source tables must respect the FK hierarchy -- parent tables are inserted first so that child table FK references are satisfied:

- `target_insert_fn` (inserting into source): `_position` before `_empdep` (because `_empdep` has FK to `_position`)
- `source_insert_fn` (inserting into target): `_city_country` before `_department_city` before `_department` before `_person` before `_person_email` / `_person_phone`

## ON CONFLICT DO NOTHING

Every INSERT in the mapping function uses `ON CONFLICT (pk_cols) DO NOTHING`. This prevents duplicate insertions when the same tuple already exists in the target table -- which can happen when multiple source tuples map to the same target row (e.g., two employees in the same department both trigger an insert of the same department row).

## The loop insertion

Before inserting into target/source tables, the mapping function inserts the reverse loop value:
- `source_insert_fn` (S-to-T): does not explicitly insert a loop value; cleanup deletes all loop rows
- `target_insert_fn` (T-to-S): inserts `VALUES (-1)` before source table inserts, so source-side INSERT triggers see `-1` and suppress re-triggering

The `desired_output.sql` variant shows both directions inserting explicit loop markers:
- `SOURCE_INSERT_FN`: no explicit reverse marker (relies on cleanup)
- `TARGET_INSERT_FN`: `INSERT INTO transducer._loop VALUES (-1)` between `_position` and `_empdep` inserts

## NOT NULL filtering

The temp table join includes WHERE clauses checking that all universal attributes are NOT NULL. This filters out incomplete tuples that result from LEFT OUTER JOINs where some tables had no matching rows:

```sql
WHERE ssn IS NOT NULL AND name IS NOT NULL AND phone IS NOT NULL
      AND email IS NOT NULL AND dep_name IS NOT NULL AND dep_address IS NOT NULL
      AND city IS NOT NULL AND country IS NOT NULL
```

## Cleanup

At the end of the mapping function, all tracking infrastructure is emptied:

```sql
-- All _INSERT tables on the triggering side
DELETE FROM schema._table1_INSERT;
DELETE FROM schema._table2_INSERT;

-- All _INSERT_JOIN tables on the triggering side
DELETE FROM schema._table1_INSERT_JOIN;
DELETE FROM schema._table2_INSERT_JOIN;

-- The loop table
DELETE FROM schema._loop;
```

This resets the system for the next insert operation. The temp table (if used) is also dropped.
