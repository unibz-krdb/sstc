# DELETE Trigger Chain

The DELETE trigger chain mirrors the INSERT chain in structure (same 4 steps using `OLD` instead of `NEW`, `_DELETE` tables instead of `_INSERT` tables), but adds an independence check at the mapping stage. When deleting a tuple from one schema side, some target tables may share attribute values with other remaining tuples, and blindly cascading deletes would remove data that is still referenced. The independence check prevents this.

---

## Chain structure (mirror of INSERT)

The DELETE chain follows the same 4 steps as the INSERT chain (see [insert-chain.md](insert-chain.md)):

**Step 1 -- Base DELETE trigger (`_DELETE_fn`):** fires AFTER DELETE on the base table, copies `OLD` row values into `_DELETE` tracking table. Loop check uses the same marker direction as INSERT (source checks for `-1`, target checks for `1`).

```sql
CREATE OR REPLACE FUNCTION schema._tablename_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * FROM schema._loop WHERE loop_start = -1) THEN
      RETURN NULL;
   ELSE
      INSERT INTO schema._tablename_DELETE VALUES(OLD.col1, OLD.col2, ...);
      RETURN NEW;
   END IF;
END; $$;
```

**Step 2 -- JOIN function (`_DELETE_JOIN_fn`):** creates universal temp table, joins `_DELETE` table with other base tables via NATURAL LEFT OUTER JOIN, projects into all `_DELETE_JOIN` tables, inserts loop marker.

```sql
CREATE OR REPLACE FUNCTION schema._tablename_DELETE_JOIN_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   CREATE TEMPORARY TABLE temp_table (...universal columns...);

   INSERT INTO temp_table (
       SELECT col1, col2, ...
       FROM schema._TABLENAME_DELETE
       NATURAL LEFT OUTER JOIN schema._OTHER_TABLE1
       NATURAL LEFT OUTER JOIN schema._OTHER_TABLE2 ...
   );

   INSERT INTO schema._table1_DELETE_JOIN (SELECT ... FROM temp_table);
   INSERT INTO schema._table2_DELETE_JOIN (SELECT ... FROM temp_table);
   INSERT INTO schema._LOOP VALUES (1);  -- or (-1) for target side
   ...

   DELETE FROM temp_table;
   DROP TABLE temp_table;
   RETURN NEW;
END; $$;
```

**Step 3 -- Mapping function:** same wait mechanism as INSERT, then performs independence-checked deletes (described below).

**Step 4 -- Cleanup:** same as INSERT -- `DELETE FROM` all `_DELETE`, `_DELETE_JOIN`, and `_LOOP` tables.

## The independence problem

When deleting a tuple from one schema side, the universal join reconstruction may produce attribute values that are shared with other, still-existing tuples. Deleting from every target table would remove data that other tuples still depend on.

### Example

Consider these source tuples:

```
EMPDEP:
  {ssn1, John, phone11, email11, dep1, depadd1}
  {ssn1, John, phone12, email11, dep1, depadd1}
  {ssn2, June, phone21, email21, dep2, depadd2}
  {ssn3, Joel, phone31, email31, dep1, depadd1}

POSITION:
  {depadd1, Paris, France}
  {depadd2, London, UK}
```

These decompose into the target schema:

```
PERSON:           {ssn1, John, dep1}, {ssn2, June, dep2}, {ssn3, Joel, dep1}
PERSON_PHONE:     {ssn1, phone11}, {ssn1, phone12}, {ssn2, phone21}, {ssn3, phone31}
PERSON_EMAIL:     {ssn1, email11}, {ssn2, email21}, {ssn3, email31}
DEPARTMENT:       {dep1, depadd1}, {dep2, depadd2}
DEPARTMENT_CITY:  {depadd1, Paris}, {depadd2, London}
CITY_COUNTRY:     {Paris, France}, {London, UK}
```

If we delete person `ssn3` from the source, the universal join reconstruction gives `{ssn3, Joel, phone31, email31, dep1, depadd1, Paris, France}`. But `ssn1` also references `dep1`, `depadd1`, `Paris`, and `France`. Deleting from DEPARTMENT, DEPARTMENT_CITY, or CITY_COUNTRY would break ssn1's data.

The correct behavior: only delete from PERSON, PERSON_PHONE, and PERSON_EMAIL (the tables whose attribute values are unique to ssn3), and leave DEPARTMENT, DEPARTMENT_CITY, and CITY_COUNTRY intact.

## The independence check

For each target table `Ti` containing attributes `ATTi` through `ATTj`, the check determines whether other tuples in the universal join still reference Ti's values. The check compares all tuples sharing the *other* attributes (everything except Ti's columns) against the deleted tuples:

### Generic pattern

```sql
IF EXISTS (
    SELECT * FROM ALL_temp
    WHERE ATT1 = NEW_temp.ATT1 AND ... AND ATTi-1 = NEW_temp.ATTi-1
          AND ATTj+1 = NEW_temp.ATTj+1 AND ...
    EXCEPT (SELECT * FROM NEW_temp)
) THEN
    -- Other tuples still depend on Ti's values: safe to delete only from Ti
    DELETE FROM Ti WHERE ATTi = NEW_temp.ATTi AND ... AND ATTj = NEW_temp.ATTj;
END IF;
```

Where:
- `ALL_temp` is the full universal join of all remaining base tuples on the source side
- `NEW_temp` (or `temp_table_join`) is the universal join reconstruction of the deleted tuples
- The WHERE clause constrains by all universal attributes *except* those belonging to `Ti`
- If the EXCEPT returns rows, other tuples still reference Ti's values through different combinations of Ti's own columns -- so Ti's specific rows can be deleted but the cascade stops there
- If the EXCEPT returns empty, no other tuples reference those values, so the delete can cascade to parent tables too

### Concrete example: source DELETE function

From `desired_output.sql`, the source delete function checks independence for PHONE and EMAIL tables, then handles the full cascade:

```sql
CREATE OR REPLACE FUNCTION transducer.source_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF NOT EXISTS (SELECT * FROM transducer._loop,
    (SELECT COUNT(*) AS rc_value FROM transducer._loop) AS row_count
    WHERE loop_start = row_count.rc_value) THEN
   RETURN NULL;
ELSE

   /* PHONE independence check */
   IF EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn AND phone = NEW.phone)) THEN
      DELETE FROM transducer._PERSON_PHONE
          WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._EMPDEP_DELETE_JOIN
                                 NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);
   END IF;

   /* EMAIL independence check */
   IF EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn AND email = NEW.email)) THEN
      DELETE FROM transducer._PERSON_EMAIL
          WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._EMPDEP_DELETE_JOIN
                                 NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);
   END IF;

   /* Full cascade: if no other tuple shares ALL attributes with the deleted one */
   IF NOT EXISTS (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn
              EXCEPT (SELECT * FROM transducer._EMPDEP WHERE ssn = NEW.ssn
               AND name = NEW.name AND phone = NEW.phone AND email = NEW.email
               AND dep_name = NEW.dep_name AND dep_address = NEW.dep_address)) THEN
      DELETE FROM transducer._PERSON_EMAIL
          WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._EMPDEP_DELETE_JOIN
                                 NATURAL LEFT OUTER JOIN transducer._POSITION_DELETE_JOIN);
      DELETE FROM transducer._PERSON_PHONE WHERE ssn = NEW.ssn AND phone = NEW.phone;
      DELETE FROM transducer._PERSON_EMAIL WHERE ssn = NEW.ssn AND email = NEW.email;
      DELETE FROM transducer._PERSON WHERE ssn = NEW.ssn AND name = NEW.name AND dep_name = NEW.dep_name;
      DELETE FROM transducer._DEPARTMENT WHERE dep_name = NEW.dep_name AND dep_address = NEW.dep_address;
   END IF;

   DELETE FROM transducer._person_DELETE;
   DELETE FROM transducer._loop;

END IF;
RETURN NEW;
END; $$;
```

### Independence check using temp tables (generalized form)

From `updates_and_more.sql`, the same logic expressed with the source-side universal join and temp tables:

```sql
IF EXISTS (
    SELECT r1.ssn, r1.name, r1.phone, r1.email, r1.dep_name, r1.dep_address, r1.city, r1.country
    FROM (transducer._POSITION NATURAL LEFT OUTER JOIN transducer._EMPDEP) AS r1, temp_table_join
    WHERE r1.dep_address = temp_table_join.dep_address
      AND r1.city = temp_table_join.city
      AND r1.country = temp_table_join.country
    EXCEPT SELECT * FROM temp_table_join
) THEN
    -- Independent: only delete from EMPDEP
    DELETE FROM transducer._EMPDEP
        WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
ELSE
    -- Not independent: cascade to POSITION too
    DELETE FROM transducer._EMPDEP
        WHERE (ssn, phone, email) IN (SELECT ssn, phone, email FROM temp_table_join);
    DELETE FROM transducer._POSITION
        WHERE (dep_address) IN (SELECT dep_address FROM temp_table_join);
END IF;
```

## Multi-table DELETE transactions

When deleting a tuple that spans multiple tables on the same schema side, the `_LOOP` table must be pre-seeded with a count equal to the number of DELETE operations. This tells the wait mechanism how many delete triggers to expect before proceeding.

```sql
BEGIN;
INSERT INTO transducer._loop VALUES (4);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn3';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn3';
DELETE FROM transducer._PERSON WHERE ssn = 'ssn3';
END;
```

Without the pre-seeded `_LOOP` value, the first DELETE trigger would fire the mapping function prematurely before all related tables have been cleaned.

For larger cascading deletes that span more tables:

```sql
BEGIN;
INSERT INTO transducer._loop VALUES (7);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn1';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn1';
DELETE FROM transducer._PERSON WHERE ssn = 'ssn1';
DELETE FROM transducer._DEPARTMENT WHERE dep_name = 'dep1';
DELETE FROM transducer._DEPARTMENT_CITY WHERE dep_address = 'depadd1';
DELETE FROM transducer._CITY_COUNTRY WHERE city = 'Paris';
END;
```

The pre-seeded value must equal the total number of `_LOOP` rows that will exist when all deletes have fired (the pre-seeded row itself plus one row per DELETE trigger that fires).
