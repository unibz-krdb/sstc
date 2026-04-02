/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._PERSON_INSERT VALUES(NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, empid, name, hdate, phone, email, dept, manager
FROM transducer._PERSON_INSERT);

INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;


CREATE OR REPLACE FUNCTION transducer.SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Something got added in a JOIN table';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on TARGET';

   INSERT INTO transducer._P (SELECT DISTINCT ssn, name FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND name IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;


   /* Okay, regarding the INSERTs into subtables, I'm not sure how to generalize this. For instance, we have a table _P which contains every persons of _PERSON and another
      two tables _PE and _PED, both defining persons with a non-null empid attribute and a non-null dept attribute for the second.
      Checking for these is easy in our current URA scenario, but in a many to many table case, I think we would recreate the join table and check it */
   
   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE_HDATE (SELECT DISTINCT empid, hdate FROM transducer._PERSON_INSERT_JOIN 
                              WHERE empid IS NOT NULL AND hdate IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._DEPT_MANAGER (SELECT DISTINCT dept, manager FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (dept) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED_DEPT (SELECT DISTINCT empid, dept FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;



   INSERT INTO transducer._PERSON_PHONE (SELECT DISTINCT ssn, phone FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

   INSERT INTO transducer._PERSON_EMAIL (SELECT DISTINCT ssn, email FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;


   DELETE FROM transducer._PERSON_INSERT;
   DELETE FROM transducer._PERSON_INSERT_JOIN;
   DELETE FROM transducer._loop;
   RETURN NEW;
END IF;
END;  $$;

/** T->S INSERTS **/

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _P';
   INSERT INTO transducer._P_INSERT VALUES(NEW.ssn, NEW.name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE';
   INSERT INTO transducer._PE_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE_HDATE';
   INSERT INTO transducer._PE_HDATE_INSERT VALUES(NEW.empid, NEW.hdate);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_HDATE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED';
   INSERT INTO transducer._PED_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED_DEPT';
   INSERT INTO transducer._PED_DEPT_INSERT VALUES(NEW.empid, NEW.dept);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_DEPT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPT_MANAGER';
   INSERT INTO transducer._DEPT_MANAGER_INSERT VALUES(NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._DEPT_MANAGER_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_PHONE';
   INSERT INTO transducer._PERSON_PHONE_INSERT VALUES(NEW.ssn, NEW.phone);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_EMAIL';
   INSERT INTO transducer._PERSON_EMAIL_INSERT VALUES(NEW.ssn, NEW.email);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

RAISE NOTICE 'Function transducer.target_insert_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _PERSON';
        
create temporary table temp_table_join (
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN transducer._PE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
   WHERE ssn IS NOT NULL AND NAME IS NOT NULL 
   AND phone IS NOT NULL AND email IS NOT NULL
   AND ((empid IS NULL AND hdate IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL)
      OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL)));

IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE empid IS NULL)) THEN
   RAISE NOTICE 'Annoying scenario in which the temp join table contains multiple correct tuples';
   IF EXISTS (SELECT * FROM temp_table_join 
         EXCEPT (SELECT * FROM temp_table_join WHERE dept IS NULL)) THEN
      DELETE FROM temp_table_join WHERE dept IS NULL;
   ELSE
      DELETE FROM temp_table_join WHERE empid IS NULL;
   END IF;     
END IF;

INSERT INTO transducer._PERSON (SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table_join) ON CONFLICT (ssn, phone, email) DO NOTHING;


DELETE FROM transducer._P_INSERT;
DELETE FROM transducer._PE_INSERT;
DELETE FROM transducer._PE_HDATE_INSERT;
DELETE FROM transducer._PED_INSERT;
DELETE FROM transducer._PED_DEPT_INSERT;
DELETE FROM transducer._DEPT_MANAGER_INSERT;
DELETE FROM transducer._PERSON_PHONE_INSERT;
DELETE FROM transducer._PERSON_EMAIL_INSERT;

DELETE FROM transducer._P_INSERT_JOIN;
DELETE FROM transducer._PE_INSERT_JOIN;
DELETE FROM transducer._PE_HDATE_INSERT_JOIN;
DELETE FROM transducer._PED_INSERT_JOIN;
DELETE FROM transducer._PED_DEPT_INSERT_JOIN;
DELETE FROM transducer._DEPT_MANAGER_INSERT_JOIN;
DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;



DELETE FROM transducer._loop;
DELETE FROM temp_table_join;
DROP TABLE temp_table_join;

RETURN NEW;
END IF;
END;    $$;